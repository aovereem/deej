package deej

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jacobsa/go-serial/serial"
	"go.uber.org/zap"

	"github.com/omriharel/deej/pkg/deej/util"
)

// SerialIO provides a deej-aware abstraction layer to managing serial I/O
type SerialIO struct {
	comPort  string
	baudRate uint

	deej   *Deej
	logger *zap.SugaredLogger

	stopChannel chan bool
	connected   bool // whether a serial connection is currently open (only mutated by the worker after Start)

	// lifecycle state for the connection worker, guarded by stateMu
	stateMu    sync.Mutex
	running    bool          // whether the connection worker goroutine is active (survives reconnects)
	workerDone chan struct{} // closed by the worker when it has fully exited; lets Stop() be synchronous

	connOptions serial.OpenOptions
	conn        io.ReadWriteCloser

	lastKnownNumSliders        int
	currentSliderPercentValues []float32

	// negotiated via the optional boot handshake; these default to backwards-compatible
	// values so firmware that never sends a handshake behaves exactly as before
	adcMax            int // full-scale ADC reading used as the divisor (1023 for 10-bit AVR)
	negotiatedSliders int // slider count declared by the device, or 0 if no handshake seen

	sliderMoveConsumers []chan SliderMoveEvent
}

// SliderMoveEvent represents a single slider move captured by deej
type SliderMoveEvent struct {
	SliderID     int
	PercentValue float32
}

// the default full-scale ADC value, assumed when the device sends no handshake (10-bit AVR)
const defaultADCMax = 1023

var expectedLinePattern = regexp.MustCompile(`^\d{1,4}(\|\d{1,4})*\r\n$`)

// optional boot handshake, e.g. "deej:hello:sliders=6:max=1023". the firmware sends this
// periodically; it lets the host pin the slider count and ADC resolution instead of
// inferring them from data frames. old firmware never sends it, and old hosts ignore it
// (it doesn't match expectedLinePattern).
var handshakePattern = regexp.MustCompile(`^deej:hello:sliders=(\d+):max=(\d+)\r?\n?$`)

// parseHandshake returns the slider count and ADC full-scale value from a handshake line,
// and ok=false if the line is not a handshake
func parseHandshake(line string) (sliders int, adcMax int, ok bool) {
	match := handshakePattern.FindStringSubmatch(line)
	if match == nil {
		return 0, 0, false
	}

	sliders, _ = strconv.Atoi(match[1])
	adcMax, _ = strconv.Atoi(match[2])
	return sliders, adcMax, true
}

// NewSerialIO creates a SerialIO instance that uses the provided deej
// instance's connection info to establish communications with the arduino chip
func NewSerialIO(deej *Deej, logger *zap.SugaredLogger) (*SerialIO, error) {
	logger = logger.Named("serial")

	sio := &SerialIO{
		deej:                deej,
		logger:              logger,
		stopChannel:         make(chan bool),
		connected:           false,
		conn:                nil,
		adcMax:              defaultADCMax,
		sliderMoveConsumers: []chan SliderMoveEvent{},
	}

	logger.Debug("Created serial i/o instance")

	// respond to config changes
	sio.setupOnConfigReload()

	return sio, nil
}

// Start attempts to connect to our arduino chip
func (sio *SerialIO) Start() error {

	// don't allow multiple concurrent connection workers
	sio.stateMu.Lock()
	if sio.running {
		sio.stateMu.Unlock()
		sio.logger.Warn("Already running, can't start another connection without stopping first")
		return errors.New("serial: connection already active")
	}
	sio.stateMu.Unlock()

	// set minimum read size according to platform (0 for windows, 1 for linux)
	// this prevents a rare bug on windows where serial reads get congested,
	// resulting in significant lag
	minimumReadSize := 0
	if util.Linux() {
		minimumReadSize = 1
	}

	sio.connOptions = serial.OpenOptions{
		PortName:        sio.deej.config.ConnectionInfo.COMPort,
		BaudRate:        uint(sio.deej.config.ConnectionInfo.BaudRate),
		DataBits:        8,
		StopBits:        1,
		MinimumReadSize: uint(minimumReadSize),
	}

	sio.logger.Debugw("Attempting serial connection",
		"comPort", sio.connOptions.PortName,
		"baudRate", sio.connOptions.BaudRate,
		"minReadSize", minimumReadSize)

	// open the connection synchronously the first time, so the caller (deej.run) can
	// react to a busy/non-existent COM port by notifying the user and quitting
	if err := sio.open(); err != nil {
		return err
	}

	// hand off to the connection worker, which reads lines and transparently
	// reconnects if the device drops (e.g. the cable is unplugged and replugged)
	sio.stateMu.Lock()
	sio.running = true
	sio.workerDone = make(chan struct{})
	sio.stateMu.Unlock()

	go sio.connectionWorker()

	return nil
}

// open establishes a serial connection using the current connOptions
func (sio *SerialIO) open() error {
	conn, err := serial.Open(sio.connOptions)
	if err != nil {

		// might need a user notification here, TBD
		sio.logger.Warnw("Failed to open serial connection", "error", err)
		return fmt.Errorf("open serial connection: %w", err)
	}

	sio.conn = conn
	sio.connected = true

	sio.logger.Named(strings.ToLower(sio.connOptions.PortName)).Infow("Connected", "conn", sio.conn)

	return nil
}

// connectionWorker reads lines from the active connection until it drops or we're
// told to stop. on an unexpected drop it attempts to reconnect with backoff, so a
// brief unplug or device reset no longer silently kills deej until a restart.
func (sio *SerialIO) connectionWorker() {
	defer sio.deej.recoverFromPanic()

	// signal completion so a concurrent Stop() can return only once we've fully exited.
	// workerDone is assigned in Start() before this goroutine is launched (happens-before).
	done := sio.workerDone
	defer func() {
		sio.stateMu.Lock()
		sio.running = false
		sio.stateMu.Unlock()
		close(done)
	}()

	for {
		namedLogger := sio.logger.Named(strings.ToLower(sio.connOptions.PortName))
		lineChannel := sio.readLine(namedLogger, bufio.NewReader(sio.conn))

		stopRequested := false

	readLoop:
		for {
			select {
			case <-sio.stopChannel:
				stopRequested = true
				break readLoop
			case line, ok := <-lineChannel:
				if !ok {
					// the reader goroutine closed the channel, meaning the read stream
					// errored out - treat it as the connection having dropped
					namedLogger.Warn("Serial read stream ended, connection appears to have dropped")
					break readLoop
				}
				sio.handleLine(namedLogger, line)
			}
		}

		// tear down the current connection, then drain any in-flight line so the
		// reader goroutine can unblock and exit cleanly (no goroutine leak)
		sio.close(namedLogger)
		for range lineChannel {
		}

		if stopRequested {
			sio.logger.Debug("Serial connection worker stopped")
			return
		}

		// the connection dropped on its own; try to get it back
		if !sio.reconnect() {
			// reconnect was aborted by a stop request
			return
		}
	}
}

// reconnect repeatedly tries to reopen the serial port with exponential backoff,
// bailing out immediately if a stop is requested. returns true once reconnected.
func (sio *SerialIO) reconnect() bool {
	const (
		initialBackoff = 500 * time.Millisecond
		maxBackoff     = 5 * time.Second
	)

	backoff := initialBackoff
	sio.logger.Warnw("Lost serial connection, will attempt to reconnect",
		"comPort", sio.connOptions.PortName)

	for {
		// wait out the backoff, but respond to a stop request without delay
		select {
		case <-sio.stopChannel:
			sio.logger.Debug("Stop requested during reconnect, aborting")
			return false
		case <-time.After(backoff):
		}

		if err := sio.open(); err != nil {
			sio.logger.Debugw("Reconnect attempt failed, will retry", "error", err, "backoff", backoff)

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		sio.logger.Infow("Reconnected to serial device", "comPort", sio.connOptions.PortName)
		return true
	}
}

// Stop signals the connection worker to shut down and blocks until it has fully
// exited, so a following Start() (e.g. on a config-driven reconnect) sees a clean state
func (sio *SerialIO) Stop() {
	sio.stateMu.Lock()
	if !sio.running {
		sio.stateMu.Unlock()
		sio.logger.Debug("Not currently running, nothing to stop")
		return
	}
	// claim the stop now so a concurrent Stop() bails out instead of sending a second
	// value the worker will never receive (which would block that caller forever)
	sio.running = false
	done := sio.workerDone
	sio.stateMu.Unlock()

	sio.logger.Debug("Shutting down serial connection")

	// the worker is always selecting on stopChannel (in its read loop or its reconnect
	// backoff), so this send is received promptly; then we wait for it to tear down
	sio.stopChannel <- true
	<-done
}

// SubscribeToSliderMoveEvents returns an unbuffered channel that receives
// a sliderMoveEvent struct every time a slider moves
func (sio *SerialIO) SubscribeToSliderMoveEvents() chan SliderMoveEvent {
	ch := make(chan SliderMoveEvent)
	sio.sliderMoveConsumers = append(sio.sliderMoveConsumers, ch)

	return ch
}

func (sio *SerialIO) setupOnConfigReload() {
	configReloadedChannel := sio.deej.config.SubscribeToChanges()

	const stopDelay = 50 * time.Millisecond

	go func() {
		defer sio.deej.recoverFromPanic()
		for {
			select {
			case <-configReloadedChannel:

				// make any config reload unset our slider number to ensure process volumes are being re-set
				// (the next read line will emit SliderMoveEvent instances for all sliders)\
				// this needs to happen after a small delay, because the session map will also re-acquire sessions
				// whenever the config file is reloaded, and we don't want it to receive these move events while the map
				// is still cleared. this is kind of ugly, but shouldn't cause any issues
				go func() {
					<-time.After(stopDelay)
					sio.lastKnownNumSliders = 0
				}()

				// if connection params have changed, attempt to stop and start the connection
				if sio.deej.config.ConnectionInfo.COMPort != sio.connOptions.PortName ||
					uint(sio.deej.config.ConnectionInfo.BaudRate) != sio.connOptions.BaudRate {

					sio.logger.Info("Detected change in connection parameters, attempting to renew connection")
					sio.Stop()

					// let the connection close
					<-time.After(stopDelay)

					if err := sio.Start(); err != nil {
						sio.logger.Warnw("Failed to renew connection after parameter change", "error", err)
					} else {
						sio.logger.Debug("Renewed connection successfully")
					}
				}
			}
		}
	}()
}

func (sio *SerialIO) close(logger *zap.SugaredLogger) {
	if sio.conn != nil {
		if err := sio.conn.Close(); err != nil {
			logger.Warnw("Failed to close serial connection", "error", err)
		} else {
			logger.Debug("Serial connection closed")
		}
	}

	sio.conn = nil
	sio.connected = false
}

func (sio *SerialIO) readLine(logger *zap.SugaredLogger, reader *bufio.Reader) chan string {
	ch := make(chan string)

	const (
		// when a non-blocking serial read momentarily has no data, bufio reports
		// io.ErrNoProgress; wait this long before retrying so we don't busy-spin the CPU
		noDataReadDelay = 5 * time.Millisecond

		// safety cap on a reassembled partial line, in case the stream never sends '\n'
		maxPendingLineBytes = 1024
	)

	go func() {
		defer sio.deej.recoverFromPanic()

		// closing the channel on exit signals the connection worker that the read
		// stream has ended (e.g. the device was unplugged), so it can reconnect
		defer close(ch)

		// bufio can return a partial line alongside io.ErrNoProgress when the non-blocking
		// read momentarily runs dry mid-frame. accumulate those partials so we only ever
		// deliver complete, newline-terminated lines - otherwise a frame split across two
		// reads would be parsed as two short frames with the wrong slider count.
		var pending strings.Builder

		for {
			chunk, err := reader.ReadString('\n')
			if err != nil {

				// io.ErrNoProgress just means the non-blocking read had no data for a
				// moment (the gap between frames, or the Arduino's post-open reset/boot
				// window). that's transient, NOT a disconnect: buffer whatever partial we
				// got and keep reading, instead of tearing down and reconnecting (which
				// would re-assert DTR, reset the board, and loop forever).
				if errors.Is(err, io.ErrNoProgress) {
					pending.WriteString(chunk)
					if pending.Len() > maxPendingLineBytes {
						pending.Reset()
					}

					time.Sleep(noDataReadDelay)
					continue
				}

				if sio.deej.Verbose() {
					logger.Warnw("Failed to read line from serial", "error", err, "line", chunk)
				}

				// genuine read error (port closed / device unplugged); closing ch
				// (deferred) tells the worker the stream ended so it can reconnect
				return
			}

			// reassemble the full line from any buffered partial plus this final chunk
			line := chunk
			if pending.Len() > 0 {
				pending.WriteString(chunk)
				line = pending.String()
				pending.Reset()
			}

			if sio.deej.Verbose() {
				logger.Debugw("Read new line", "line", line)
			}

			// deliver the complete line to the channel
			ch <- line
		}
	}()

	return ch
}

func (sio *SerialIO) handleLine(logger *zap.SugaredLogger, line string) {

	// an optional handshake line announces the device's slider count and ADC resolution.
	// check it before the numeric data pattern; firmware without a handshake never sends it
	if sliders, adcMax, ok := parseHandshake(line); ok {
		if adcMax > 0 && adcMax != sio.adcMax {
			logger.Infow("Device declared ADC resolution via handshake", "adcMax", adcMax)
			sio.adcMax = adcMax
		}
		if sliders > 0 && sliders != sio.negotiatedSliders {
			logger.Infow("Device declared slider count via handshake", "sliders", sliders)
			sio.negotiatedSliders = sliders

			// force the next data frame to re-emit move events for every slider
			sio.lastKnownNumSliders = 0
		}
		return
	}

	// this function receives an unsanitized line which is guaranteed to end with LF,
	// but most lines will end with CRLF. it may also have garbage instead of
	// deej-formatted values, so we must check for that! just ignore bad ones
	if !expectedLinePattern.MatchString(line) {
		return
	}

	// trim the suffix
	line = strings.TrimSuffix(line, "\r\n")

	// split on pipe (|), this gives a slice of numerical strings between "0" and adcMax
	splitLine := strings.Split(line, "|")
	numSliders := len(splitLine)

	// if the device declared its slider count via handshake, drop frames that don't match
	// it (a torn/corrupt frame) instead of silently re-binding sliders to the wrong count
	if sio.negotiatedSliders > 0 && numSliders != sio.negotiatedSliders {
		sio.logger.Debugw("Ignoring serial frame with unexpected slider count",
			"got", numSliders, "expected", sio.negotiatedSliders)
		return
	}

	// update our slider count, if needed - this will send slider move events for all
	if numSliders != sio.lastKnownNumSliders {
		logger.Infow("Detected sliders", "amount", numSliders)
		sio.lastKnownNumSliders = numSliders
		sio.currentSliderPercentValues = make([]float32, numSliders)

		// reset everything to be an impossible value to force the slider move event later
		for idx := range sio.currentSliderPercentValues {
			sio.currentSliderPercentValues[idx] = -1.0
		}
	}

	// for each slider:
	moveEvents := []SliderMoveEvent{}
	for sliderIdx, stringValue := range splitLine {

		// convert string values to integers ("1023" -> 1023)
		number, _ := strconv.Atoi(stringValue)

		// turns out the first line could come out dirty sometimes (i.e. "4558|925|41|643|220")
		// so let's check the first number for correctness just in case
		if sliderIdx == 0 && number > sio.adcMax {
			sio.logger.Debugw("Got malformed line from serial, ignoring", "line", line)
			return
		}

		// map the value from raw to a "dirty" float between 0 and 1 (e.g. 0.15451...)
		dirtyFloat := float32(number) / float32(sio.adcMax)

		// normalize it to an actual volume scalar between 0.0 and 1.0 with 2 points of precision
		normalizedScalar := util.NormalizeScalar(dirtyFloat)

		// if sliders are inverted, take the complement of 1.0
		if sio.deej.config.InvertSliders {
			normalizedScalar = 1 - normalizedScalar
		}

		// check if it changes the desired state (could just be a jumpy raw slider value)
		if util.SignificantlyDifferent(sio.currentSliderPercentValues[sliderIdx], normalizedScalar, sio.deej.config.NoiseReductionLevel) {

			// if it does, update the saved value and create a move event
			sio.currentSliderPercentValues[sliderIdx] = normalizedScalar

			moveEvents = append(moveEvents, SliderMoveEvent{
				SliderID:     sliderIdx,
				PercentValue: normalizedScalar,
			})

			if sio.deej.Verbose() {
				logger.Debugw("Slider moved", "event", moveEvents[len(moveEvents)-1])
			}
		}
	}

	// deliver move events if there are any, towards all potential consumers
	if len(moveEvents) > 0 {
		for _, consumer := range sio.sliderMoveConsumers {
			for _, moveEvent := range moveEvents {
				consumer <- moveEvent
			}
		}
	}
}

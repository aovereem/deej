package deej

import (
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	yaml "gopkg.in/yaml.v2"

	"github.com/omriharel/deej/pkg/deej/util"
)

// CanonicalConfig provides application-wide access to configuration fields,
// as well as loading/file watching logic for deej's configuration file
type CanonicalConfig struct {
	SliderMapping *sliderMap

	ConnectionInfo struct {
		COMPort  string
		BaudRate int
	}

	InvertSliders bool

	NoiseReductionLevel string

	logger             *zap.SugaredLogger
	notifier           Notifier
	stopWatcherChannel chan bool

	reloadConsumers []chan bool

	userConfig     *viper.Viper
	internalConfig *viper.Viper
}

const (
	userConfigFilepath     = "config.yaml"
	internalConfigFilepath = "preferences.yaml"

	userConfigName     = "config"
	internalConfigName = "preferences"

	userConfigPath = "."

	configType = "yaml"

	configKeySliderMapping       = "slider_mapping"
	configKeyInvertSliders       = "invert_sliders"
	configKeyCOMPort             = "com_port"
	configKeyBaudRate            = "baud_rate"
	configKeyNoiseReductionLevel = "noise_reduction"

	// accepted noise_reduction values (see util.SignificantlyDifferent)
	noiseReductionLow     = "low"
	noiseReductionDefault = "default"
	noiseReductionHigh    = "high"

	defaultCOMPort  = "COM4"
	defaultBaudRate = 9600
)

// knownUserConfigKeys is the set of recognized top-level keys in config.yaml,
// used to warn the user about typos and unknown settings
var knownUserConfigKeys = map[string]bool{
	configKeySliderMapping:       true,
	configKeyInvertSliders:       true,
	configKeyCOMPort:             true,
	configKeyBaudRate:            true,
	configKeyNoiseReductionLevel: true,
}

// has to be defined as a non-constant because we're using path.Join
var internalConfigPath = path.Join(".", logDirectory)

var defaultSliderMapping = func() *sliderMap {
	emptyMap := newSliderMap()
	emptyMap.set(0, []string{masterSessionName})

	return emptyMap
}()

// NewConfig creates a config instance for the deej object and sets up viper instances for deej's config files
func NewConfig(logger *zap.SugaredLogger, notifier Notifier) (*CanonicalConfig, error) {
	logger = logger.Named("config")

	cc := &CanonicalConfig{
		logger:             logger,
		notifier:           notifier,
		reloadConsumers:    []chan bool{},
		stopWatcherChannel: make(chan bool),
	}

	// distinguish between the user-provided config (config.yaml) and the internal config (logs/preferences.yaml)
	userConfig := viper.New()
	userConfig.SetConfigName(userConfigName)
	userConfig.SetConfigType(configType)
	userConfig.AddConfigPath(userConfigPath)

	userConfig.SetDefault(configKeySliderMapping, map[string][]string{})
	userConfig.SetDefault(configKeyInvertSliders, false)
	userConfig.SetDefault(configKeyCOMPort, defaultCOMPort)
	userConfig.SetDefault(configKeyBaudRate, defaultBaudRate)
	userConfig.SetDefault(configKeyNoiseReductionLevel, noiseReductionDefault)

	internalConfig := viper.New()
	internalConfig.SetConfigName(internalConfigName)
	internalConfig.SetConfigType(configType)
	internalConfig.AddConfigPath(internalConfigPath)

	cc.userConfig = userConfig
	cc.internalConfig = internalConfig

	logger.Debug("Created config instance")

	return cc, nil
}

// Load reads deej's config files from disk and tries to parse them
func (cc *CanonicalConfig) Load() error {
	cc.logger.Debugw("Loading config", "path", userConfigFilepath)

	// make sure it exists
	if !util.FileExists(userConfigFilepath) {
		cc.logger.Warnw("Config file not found", "path", userConfigFilepath)
		cc.notifier.Notify("Can't find configuration!",
			fmt.Sprintf("%s must be in the same directory as deej. Please re-launch", userConfigFilepath))

		return fmt.Errorf("config file doesn't exist: %s", userConfigFilepath)
	}

	// load the user config
	if err := cc.userConfig.ReadInConfig(); err != nil {
		cc.logger.Warnw("Viper failed to read user config", "error", err)

		// if the error is yaml-format-related, show a sensible error. otherwise, show 'em to the logs
		if strings.Contains(err.Error(), "yaml:") {
			cc.notifier.Notify("Invalid configuration!",
				fmt.Sprintf("Please make sure %s is in a valid YAML format.", userConfigFilepath))
		} else {
			cc.notifier.Notify("Error loading configuration!", "Please check deej's logs for more details.")
		}

		return fmt.Errorf("read user config: %w", err)
	}

	// viper silently keeps only the last of any duplicate key and ignores unknown keys,
	// so re-inspect the raw file to warn the user about misconfiguration (non-fatal)
	cc.validateUserConfigFile()

	// load the internal config - this doesn't have to exist, so it can error
	if err := cc.internalConfig.ReadInConfig(); err != nil {
		cc.logger.Debugw("Viper failed to read internal config", "error", err, "reminder", "this is fine")
	}

	// canonize the configuration with viper's helpers
	if err := cc.populateFromVipers(); err != nil {
		cc.logger.Warnw("Failed to populate config fields", "error", err)
		return fmt.Errorf("populate config fields: %w", err)
	}

	cc.logger.Info("Loaded config successfully")
	cc.logger.Infow("Config values",
		"sliderMapping", cc.SliderMapping,
		"connectionInfo", cc.ConnectionInfo,
		"invertSliders", cc.InvertSliders)

	return nil
}

// SubscribeToChanges allows external components to receive updates when the config is reloaded
func (cc *CanonicalConfig) SubscribeToChanges() chan bool {
	c := make(chan bool)
	cc.reloadConsumers = append(cc.reloadConsumers, c)

	return c
}

// WatchConfigFileChanges starts watching for configuration file changes
// and attempts reloading the config when they happen
func (cc *CanonicalConfig) WatchConfigFileChanges() {
	cc.logger.Debugw("Starting to watch user config file for changes", "path", userConfigFilepath)

	const (
		minTimeBetweenReloadAttempts = time.Millisecond * 500
		delayBetweenEventAndReload   = time.Millisecond * 50
	)

	lastAttemptedReload := time.Now()

	// establish watch using viper as opposed to doing it ourselves, though our internal cooldown is still required
	cc.userConfig.WatchConfig()
	cc.userConfig.OnConfigChange(func(event fsnotify.Event) {

		// when we get a write event...
		if event.Op&fsnotify.Write == fsnotify.Write {

			now := time.Now()

			// ... check if it's not a duplicate (many editors will write to a file twice)
			if lastAttemptedReload.Add(minTimeBetweenReloadAttempts).Before(now) {

				// and attempt reload if appropriate
				cc.logger.Debugw("Config file modified, attempting reload", "event", event)

				// wait a bit to let the editor actually flush the new file contents to disk
				<-time.After(delayBetweenEventAndReload)

				if err := cc.Load(); err != nil {
					cc.logger.Warnw("Failed to reload config file", "error", err)
				} else {
					cc.logger.Info("Reloaded config successfully")
					cc.notifier.Notify("Configuration reloaded!", "Your changes have been applied.")

					cc.onConfigReloaded()
				}

				// don't forget to update the time
				lastAttemptedReload = now
			}
		}
	})

	// wait till they stop us
	<-cc.stopWatcherChannel
	cc.logger.Debug("Stopping user config file watcher")
	cc.userConfig.OnConfigChange(nil)
}

// StopWatchingConfigFile signals our filesystem watcher to stop
func (cc *CanonicalConfig) StopWatchingConfigFile() {
	cc.stopWatcherChannel <- true
}

// validateUserConfigFile re-parses the raw config file (viper collapses duplicate keys
// and drops unknown ones silently) to surface misconfiguration to the user. it never fails
// the load - the config is still usable - it just warns/notifies so problems aren't invisible.
func (cc *CanonicalConfig) validateUserConfigFile() {
	raw, err := os.ReadFile(userConfigFilepath)
	if err != nil {
		cc.logger.Debugw("Couldn't re-read config file for validation, skipping", "error", err)
		return
	}

	// MapSlice preserves order and, crucially, duplicate top-level keys
	var doc yaml.MapSlice
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		// viper already surfaced parse errors to the user; nothing more to do here
		return
	}

	keyCounts := map[string]int{}
	for _, item := range doc {
		key, ok := item.Key.(string)
		if !ok {
			continue
		}

		keyCounts[key]++

		if !knownUserConfigKeys[key] {
			cc.logger.Warnw("Ignoring unknown config key (typo?)", "key", key)
		}
	}

	for key, count := range keyCounts {
		if count > 1 {
			cc.logger.Warnw("Duplicate config key found, only the last occurrence takes effect",
				"key", key, "occurrences", count)
			cc.notifier.Notify("Duplicate setting in configuration!",
				fmt.Sprintf("'%s' appears %d times in %s - only the last one is used. Please remove the duplicates.",
					key, count, userConfigFilepath))
		}
	}
}

func (cc *CanonicalConfig) populateFromVipers() error {

	// merge the slider mappings from the user and internal configs
	var invalidSliderKeys []string
	cc.SliderMapping, invalidSliderKeys = sliderMapFromConfigs(
		cc.userConfig.GetStringMapStringSlice(configKeySliderMapping),
		cc.internalConfig.GetStringMapStringSlice(configKeySliderMapping),
	)

	// a non-numeric or negative slider index used to silently fold into slider 0,
	// clobbering its mapping - warn instead so the user can fix the typo
	if len(invalidSliderKeys) > 0 {
		cc.logger.Warnw("Ignoring slider mappings with invalid (non-numeric or negative) indices",
			"keys", invalidSliderKeys)
		cc.notifier.Notify("Invalid slider mapping!",
			fmt.Sprintf("These slider numbers in %s aren't valid and were ignored: %s",
				userConfigFilepath, strings.Join(invalidSliderKeys, ", ")))
	}

	// get the rest of the config fields - viper saves us a lot of effort here
	cc.ConnectionInfo.COMPort = cc.userConfig.GetString(configKeyCOMPort)

	cc.ConnectionInfo.BaudRate = cc.userConfig.GetInt(configKeyBaudRate)
	if cc.ConnectionInfo.BaudRate <= 0 {
		cc.logger.Warnw("Invalid baud rate specified, using default value",
			"key", configKeyBaudRate,
			"invalidValue", cc.ConnectionInfo.BaudRate,
			"defaultValue", defaultBaudRate)

		cc.ConnectionInfo.BaudRate = defaultBaudRate
	}

	cc.InvertSliders = cc.userConfig.GetBool(configKeyInvertSliders)

	cc.NoiseReductionLevel = cc.userConfig.GetString(configKeyNoiseReductionLevel)
	switch cc.NoiseReductionLevel {
	case noiseReductionLow, noiseReductionDefault, noiseReductionHigh:
		// recognized value, nothing to do
	default:
		cc.logger.Warnw("Invalid noise_reduction value, using default",
			"key", configKeyNoiseReductionLevel,
			"invalidValue", cc.NoiseReductionLevel,
			"validValues", []string{noiseReductionLow, noiseReductionDefault, noiseReductionHigh})

		cc.NoiseReductionLevel = noiseReductionDefault
	}

	cc.logger.Debug("Populated config fields from vipers")

	return nil
}

func (cc *CanonicalConfig) onConfigReloaded() {
	cc.logger.Debug("Notifying consumers about configuration reload")

	for _, consumer := range cc.reloadConsumers {
		consumer <- true
	}
}

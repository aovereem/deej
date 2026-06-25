// deej firmware with a boot handshake.
//
// On top of the standard pipe-delimited slider stream, this sketch periodically sends a
// handshake line that declares the slider count and the ADC full-scale value, e.g.:
//
//     deej:hello:sliders=5:max=1023
//
// The host uses it to pin the slider count and ADC resolution instead of inferring them
// from data frames. This is fully backwards compatible: a host that doesn't understand the
// handshake simply ignores that line (it isn't numeric), and older firmware that never
// sends a handshake makes the host fall back to its previous behaviour.
//
// Porting to a 12-bit board (ESP32 / RP2040 / SAMD / Teensy):
//   - set ADC_MAX to 4095 (or your board's full-scale value)
//   - call analogReadResolution(12) in setup()
// The host will read `max=4095` from the handshake and scale correctly with no host changes.

const int NUM_SLIDERS = 5;                          // set this to your actual slider count
const int analogInputs[NUM_SLIDERS] = {A0, A1, A2, A3, A4};
const long ADC_MAX = 1023;                          // 10-bit AVR ADC full-scale; 4095 for 12-bit boards

const unsigned long HANDSHAKE_INTERVAL_MS = 2000;   // re-announce so a host that connects late still learns our capabilities

int analogSliderValues[NUM_SLIDERS];
unsigned long lastHandshake = 0;

void setup() {
  for (int i = 0; i < NUM_SLIDERS; i++) {
    pinMode(analogInputs[i], INPUT);
  }

  Serial.begin(9600);

  // announce ourselves immediately on boot
  sendHandshake();
  lastHandshake = millis();
}

void loop() {
  unsigned long now = millis();
  if (now - lastHandshake >= HANDSHAKE_INTERVAL_MS) {
    sendHandshake();
    lastHandshake = now;
  }

  updateSliderValues();
  sendSliderValues();
  delay(10);
}

void sendHandshake() {
  // host pins slider count + ADC resolution from this line; old hosts ignore it
  Serial.print("deej:hello:sliders=");
  Serial.print(NUM_SLIDERS);
  Serial.print(":max=");
  Serial.println(ADC_MAX);
}

void updateSliderValues() {
  for (int i = 0; i < NUM_SLIDERS; i++) {
    analogSliderValues[i] = analogRead(analogInputs[i]);
  }
}

void sendSliderValues() {
  // build into a fixed buffer instead of String to avoid heap fragmentation on AVR.
  // worst case per field is "4095|" (5 chars); 8 bytes/field is comfortable headroom.
  char builtString[NUM_SLIDERS * 8 + 1];
  int pos = 0;

  for (int i = 0; i < NUM_SLIDERS; i++) {
    if (i > 0) {
      builtString[pos++] = '|';
    }
    pos += snprintf(builtString + pos, sizeof(builtString) - pos, "%d", analogSliderValues[i]);
  }

  Serial.println(builtString);
}

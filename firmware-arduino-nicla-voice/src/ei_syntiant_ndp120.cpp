/* The Clear BSD License
 *
 * Copyright (c) 2025 EdgeImpulse Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted (subject to the limitations in the disclaimer
 * below) provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 *   * Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the following disclaimer in the
 *   documentation and/or other materials provided with the distribution.
 *
 *   * Neither the name of the copyright holder nor the names of its
 *   contributors may be used to endorse or promote products derived from this
 *   software without specific prior written permission.
 *
 * NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY
 * THIS LICENSE. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
/* Include ----------------------------------------------------------------- */
#include "ei_syntiant_ndp120.h"
#include "Nicla_System.h"
#include "NDP.h"
#include "edge-impulse-sdk/porting/ei_classifier_porting.h"
#include "ingestion-sdk-platform/nicla_syntiant/ei_at_handlers.h"
#include "ingestion-sdk-platform/nicla_syntiant/ei_device_syntiant_nicla.h"
#include "ingestion-sdk-platform/sensors/ei_inertial.h"
#include "inference/ei_run_impulse.h"
#ifdef WITH_IMU
#include "ingestion-sdk-platform/sensors/ei_inertial.h"
#include "model-parameters/model_metadata.h"
#else
#include "ingestion-sdk-platform/sensors/ei_microphone.h"
#endif
#include "BMI270_Init.h"
#include "rtos.h"
#include "Thread.h"
#include "EventQueue.h"
#define TEST_READ_TANK      0
#define READ_START_ADDRESS 0x0C
#define READ_BYTE_COUNT 16
#define SENSOR_DATA_LENGTH 16

#define ACCEL_SCALE_FACTOR ((2.0 / 32767.0) * 9.8)

#define GYRO_SCALE_FACTOR (1 / 16.4)

#define CHECK_STATUS(s) \
  do { \
    if (s) { \
      Serial.print("SPI access error in line "); \
      Serial.print(__LINE__); \
      Serial.print(" status="); \
      Serial.println(s); \
      return; \
    } \
  } while (0)
#include "mbed.h"

void limpa_fila();

float shakeThreshold = 20.0;

extern NDPClass NDP;
static ATServer *at;
static bool ndp_is_init;

#if TEST_READ_TANK == 1
static void test_ndp_extract(void);
#endif


static void error_event(void);
static void match_event(char* label);
static void irq_event(void);

enum event_type_t {
    EVENT_SNORING,
    EVENT_SUPINA_ENTER,
    EVENT_SUPINA_EXIT
};

enum pending_event_type_t {
    PENDING_EVENT_MATCH
};

typedef struct {
    unsigned long timestamp_ms;
    event_type_t type;
} event_record_t;

typedef struct {
    pending_event_type_t type;
    unsigned long timestamp_ms;
    char label[32];
} pending_event_t;

typedef struct {
    unsigned long timestamp_ms;
    event_type_t type;
} flash_event_t;

static constexpr size_t EVENT_LOG_CAPACITY = 64;
static constexpr size_t PENDING_EVENT_CAPACITY = 8;
static constexpr size_t FLASH_EVENT_QUEUE_CAPACITY = 16;
static constexpr size_t SNORE_EVENT_QUEUE_CAPACITY = 8;
static constexpr size_t FLASH_EVENT_BATCH_THRESHOLD = 5;
static constexpr unsigned long FLASH_EVENT_FLUSH_INTERVAL_MS = 2000;
static constexpr unsigned long SNORE_WINDOW_MS = 6000;
static constexpr size_t SNORE_TRIGGER_COUNT = 4;
static event_record_t event_log[EVENT_LOG_CAPACITY];
static pending_event_t pending_events[PENDING_EVENT_CAPACITY];
static flash_event_t flash_event_queue[FLASH_EVENT_QUEUE_CAPACITY];
static unsigned long snore_event_times[SNORE_EVENT_QUEUE_CAPACITY];
static size_t event_log_head = 0;
static size_t event_log_count = 0;
static const char* EVENT_CSV_PATH = "/fs/events.csv";
static bool event_csv_ready = false;
static volatile uint8_t pending_event_write_idx = 0;
static volatile uint8_t pending_event_read_idx = 0;
static volatile unsigned long pending_event_overflow_count = 0;
static size_t flash_event_write_idx = 0;
static size_t flash_event_read_idx = 0;
static size_t flash_event_count = 0;
static unsigned long flash_event_overflow_count = 0;
static unsigned long flash_write_error_count = 0;
static unsigned long last_csv_flush_ms = 0;
static size_t snore_event_head = 0;
static size_t snore_event_count = 0;
static unsigned long snore_event_overflow_count = 0;

static const char* event_type_to_string(event_type_t type);
static void record_event(event_type_t type, unsigned long timestamp_ms);
static void ensure_event_csv_ready(void);
static void dump_ram_events(void);
static void dump_csv_events(void);
static void clear_events(void);
static void enqueue_pending_match_event(const char *label, unsigned long timestamp_ms);
static void process_pending_match_events(void);
static void enqueue_flash_event(event_type_t type, unsigned long timestamp_ms);
static void process_event_csv_flush(bool force_flush);
static void enqueue_snore_timestamp(unsigned long timestamp_ms);
static void discard_old_snore_timestamps(unsigned long now_ms);
static bool has_recent_snore_burst(void);

static bool _on_match_enabled = false;
static volatile bool got_match = false;
static volatile bool got_event = false;
unsigned long instante_anterior=0; 
const unsigned long tempo_motor=2000; 
unsigned long instante_atual=0;
unsigned long ultimo_alerta_supina=0;
const unsigned long intervalo_alerta_supina=8000;
/* Public functions -------------------------------------------------------- */
/**
 * @brief 
 * 
 */
 unsigned long anteriormillis=0;

static const char* event_type_to_string(event_type_t type)
{
    switch (type) {
        case EVENT_SNORING:
            return "snoring";
        case EVENT_SUPINA_ENTER:
            return "supina_enter";
        case EVENT_SUPINA_EXIT:
            return "supina_exit";
        default:
            return "unknown";
    }
}

static void record_event(event_type_t type, unsigned long timestamp_ms)
{
    event_log[event_log_head].timestamp_ms = timestamp_ms;
    event_log[event_log_head].type = type;
    event_log_head = (event_log_head + 1) % EVENT_LOG_CAPACITY;

    if (event_log_count < EVENT_LOG_CAPACITY) {
        event_log_count++;
    }

    enqueue_flash_event(type, timestamp_ms);

    Serial.print("EVENTO: ");
    Serial.print(event_type_to_string(type));
    Serial.print(" em ");
    Serial.print(timestamp_ms);
    Serial.println(" ms");
}

static void ensure_event_csv_ready(void)
{
    if (event_csv_ready) {
        return;
    }

    FILE *fp = fopen(EVENT_CSV_PATH, "r");
    if (fp != nullptr) {
        fclose(fp);
        event_csv_ready = true;
        return;
    }

    fp = fopen(EVENT_CSV_PATH, "w");
    if (fp != nullptr) {
        fprintf(fp, "timestamp_ms,event_type\n");
        fflush(fp);
        fclose(fp);
        event_csv_ready = true;
        Serial.print("CSV EVENTOS CRIADO: ");
        Serial.println(EVENT_CSV_PATH);
    }
    else {
        Serial.print("ERRO AO CRIAR CSV DE EVENTOS: ");
        Serial.println(EVENT_CSV_PATH);
    }
}

static void dump_ram_events(void)
{
    Serial.println("RAM EVENTS BEGIN");
    Serial.print("count=");
    Serial.println(event_log_count);
    Serial.print("pending_overflow=");
    Serial.println(pending_event_overflow_count);
    Serial.print("flash_queue_count=");
    Serial.println(flash_event_count);
    Serial.print("flash_overflow=");
    Serial.println(flash_event_overflow_count);
    Serial.print("flash_write_errors=");
    Serial.println(flash_write_error_count);
    Serial.print("snore_queue_count=");
    Serial.println(snore_event_count);
    Serial.print("snore_queue_overflow=");
    Serial.println(snore_event_overflow_count);

    if (event_log_count == 0) {
        Serial.println("RAM EVENTS EMPTY");
        Serial.println("RAM EVENTS END");
        return;
    }

    size_t start = (event_log_head + EVENT_LOG_CAPACITY - event_log_count) % EVENT_LOG_CAPACITY;

    for (size_t i = 0; i < event_log_count; i++) {
        size_t idx = (start + i) % EVENT_LOG_CAPACITY;
        Serial.print(event_log[idx].timestamp_ms);
        Serial.print(",");
        Serial.println(event_type_to_string(event_log[idx].type));
    }

    Serial.println("RAM EVENTS END");
}

static void dump_csv_events(void)
{
    process_event_csv_flush(true);
    ensure_event_csv_ready();

    FILE *fp = fopen(EVENT_CSV_PATH, "r");
    if (fp == nullptr) {
        Serial.println("CSV EVENTS ERROR");
        return;
    }

    Serial.println("CSV EVENTS BEGIN");

    char line[128];
    while (fgets(line, sizeof(line), fp) != nullptr) {
        Serial.print(line);
        size_t len = strlen(line);
        if (len == 0 || line[len - 1] != '\n') {
            Serial.println();
        }
    }

    fclose(fp);
    Serial.println("CSV EVENTS END");
}

static void clear_events(void)
{
    event_log_head = 0;
    event_log_count = 0;
    pending_event_write_idx = 0;
    pending_event_read_idx = 0;
    pending_event_overflow_count = 0;
    flash_event_write_idx = 0;
    flash_event_read_idx = 0;
    flash_event_count = 0;
    flash_event_overflow_count = 0;
    flash_write_error_count = 0;
    last_csv_flush_ms = millis();
    snore_event_head = 0;
    snore_event_count = 0;
    snore_event_overflow_count = 0;

    if (remove(EVENT_CSV_PATH) == 0) {
        Serial.println("CSV EVENTS REMOVED");
    }
    else {
        Serial.println("CSV EVENTS REMOVE SKIPPED");
    }

    event_csv_ready = false;
    ensure_event_csv_ready();
    Serial.println("EVENTS CLEARED");
}

static void enqueue_flash_event(event_type_t type, unsigned long timestamp_ms)
{
    if (flash_event_count >= FLASH_EVENT_QUEUE_CAPACITY) {
        flash_event_overflow_count++;
        return;
    }

    flash_event_queue[flash_event_write_idx].timestamp_ms = timestamp_ms;
    flash_event_queue[flash_event_write_idx].type = type;
    flash_event_write_idx = (flash_event_write_idx + 1) % FLASH_EVENT_QUEUE_CAPACITY;
    flash_event_count++;
}

static void process_event_csv_flush(bool force_flush)
{
    unsigned long now = millis();

    if (flash_event_count == 0) {
        last_csv_flush_ms = now;
        return;
    }

    if (!force_flush &&
        flash_event_count < FLASH_EVENT_BATCH_THRESHOLD &&
        (now - last_csv_flush_ms) < FLASH_EVENT_FLUSH_INTERVAL_MS) {
        return;
    }

    ensure_event_csv_ready();

    FILE *fp = fopen(EVENT_CSV_PATH, "a");
    if (fp == nullptr) {
        flash_write_error_count++;
        return;
    }

    size_t read_idx = flash_event_read_idx;
    size_t queued_count = flash_event_count;

    for (size_t i = 0; i < queued_count; i++) {
        flash_event_t event = flash_event_queue[read_idx];
        if (fprintf(fp, "%lu,%s\n", event.timestamp_ms, event_type_to_string(event.type)) < 0) {
            fclose(fp);
            flash_write_error_count++;
            return;
        }
        read_idx = (read_idx + 1) % FLASH_EVENT_QUEUE_CAPACITY;
    }

    fflush(fp);
    fclose(fp);

    flash_event_read_idx = read_idx;
    flash_event_count = 0;
    last_csv_flush_ms = now;
}

static void enqueue_snore_timestamp(unsigned long timestamp_ms)
{
    if (snore_event_count >= SNORE_EVENT_QUEUE_CAPACITY) {
        snore_event_head = (snore_event_head + 1) % SNORE_EVENT_QUEUE_CAPACITY;
        snore_event_count--;
        snore_event_overflow_count++;
    }

    size_t write_idx = (snore_event_head + snore_event_count) % SNORE_EVENT_QUEUE_CAPACITY;
    snore_event_times[write_idx] = timestamp_ms;
    snore_event_count++;
}

static void discard_old_snore_timestamps(unsigned long now_ms)
{
    while (snore_event_count > 0) {
        unsigned long oldest = snore_event_times[snore_event_head];
        if ((now_ms - oldest) <= SNORE_WINDOW_MS) {
            break;
        }

        snore_event_head = (snore_event_head + 1) % SNORE_EVENT_QUEUE_CAPACITY;
        snore_event_count--;
    }
}

static bool has_recent_snore_burst(void)
{
    return snore_event_count >= SNORE_TRIGGER_COUNT;
}

static void enqueue_pending_match_event(const char *label, unsigned long timestamp_ms)
{
    uint8_t write_idx = pending_event_write_idx;
    uint8_t next_idx = (uint8_t)((write_idx + 1) % PENDING_EVENT_CAPACITY);

    if (next_idx == pending_event_read_idx) {
        pending_event_overflow_count++;
        return;
    }

    pending_events[write_idx].type = PENDING_EVENT_MATCH;
    pending_events[write_idx].timestamp_ms = timestamp_ms;
    strncpy(pending_events[write_idx].label, label, sizeof(pending_events[write_idx].label) - 1);
    pending_events[write_idx].label[sizeof(pending_events[write_idx].label) - 1] = '\0';

    pending_event_write_idx = next_idx;
}

static void process_pending_match_events(void)
{
    while (pending_event_read_idx != pending_event_write_idx) {
        pending_event_t event = pending_events[pending_event_read_idx];
        pending_event_read_idx = (uint8_t)((pending_event_read_idx + 1) % PENDING_EVENT_CAPACITY);

        if (event.type == PENDING_EVENT_MATCH) {
            nicla::leds.setColor(blue);

            if (strcmp(event.label, "NN0:Snoring") == 0) {
                Serial.print("RONCO DETETADO EM: ");
                Serial.print(event.timestamp_ms);
                Serial.println(" ms");
                record_event(EVENT_SNORING, event.timestamp_ms);
                enqueue_snore_timestamp(event.timestamp_ms);
                discard_old_snore_timestamps(event.timestamp_ms);

                if (has_recent_snore_burst()) {
                    digitalWrite(10, HIGH);
                    instante_anterior = millis();
                }
            }

            Serial.print("MATCH: ");
            Serial.println(event.label);
        }
    }
}

void setup2(){

  int status;
  uint8_t __attribute__((aligned(4))) sensor_data[SENSOR_DATA_LENGTH];

  status = NDP.sensorBMI270Read(0x0, 1, sensor_data);
  CHECK_STATUS(status);
  status = NDP.sensorBMI270Read(0x0, 1, sensor_data);
  CHECK_STATUS(status);

  status = NDP.sensorBMI270Write(0x7E, 0xB6);
  CHECK_STATUS(status);
  delay(20);

  status = NDP.sensorBMI270Read(0x0, 1, sensor_data);
  CHECK_STATUS(status);
  status = NDP.sensorBMI270Read(0x0, 1, sensor_data);
  CHECK_STATUS(status);

  status = NDP.sensorBMI270Write(0x7C, 0x00);
  CHECK_STATUS(status);
  delay(20);

  status = NDP.sensorBMI270Write(0x59, 0x00);
  CHECK_STATUS(status);

  Serial.println("- BMI270 initialization starting...");
  status = NDP.sensorBMI270Write(0x5E, sizeof(bmi270_maximum_fifo_config_file), (uint8_t*)bmi270_maximum_fifo_config_file);
  CHECK_STATUS(status);
  Serial.println("- BMI270 Initialization done!");
  status = NDP.sensorBMI270Write(0x59, 0x01);
  CHECK_STATUS(status);
  delay(200);

  status = NDP.sensorBMI270Read(0x21, 1, sensor_data);
  CHECK_STATUS(status);

  status = NDP.sensorBMI270Write(0x7D, 0x0E);  
  CHECK_STATUS(status);
  status = NDP.sensorBMI270Write(0x40, 0xA8);  
  CHECK_STATUS(status);
  status = NDP.sensorBMI270Write(0x41, 0x00);  
  CHECK_STATUS(status);
  status = NDP.sensorBMI270Write(0x42, 0xA9); 
  CHECK_STATUS(status);
  status = NDP.sensorBMI270Write(0x43, 0x00);  
  CHECK_STATUS(status);
}
void ei_setup(char* fw1, char* fw2, char* fw3)
{
    pinMode(10, OUTPUT);
    uint8_t valid_synpkg = 0;    
    bool board_flashed = false;
    uint8_t flashed_count = 0;
    EiDeviceSyntiantNicla *dev = static_cast<EiDeviceSyntiantNicla*>(EiDeviceInfo::get_device());
    char* ptr_fw[] = {fw1, fw2, fw3};
    
    ndp_is_init = false;
    Serial.begin(115200);

    nicla::begin();
    nicla::leds.begin();
    nicla::enableCharging(10);
    while (!Serial) {   
        nicla::leds.setColor(red);
    }
    
    ei_printf("Hello from Edge Impulse on Arduino Nicla Voice\r\n"
            "Compiled on %s %s\r\n",
            __DATE__,
            __TIME__);

    nicla::leds.setColor(green);
    NDP.onEvent(irq_event);
    NDP.onMatch(match_event);

    dev->get_ext_flash()->init_fs();
    for (int8_t i = 0; i < 3 ; i++) {
        if (ptr_fw[i] != nullptr){                 
            if (dev->get_file_exist(ptr_fw[i])){
                ei_printf("%s exist\n", ptr_fw[i]);
                valid_synpkg++;
            }
            else{
                ei_printf("%s not found!\n", ptr_fw[i]);
            }
        }
    }
   

    if (valid_synpkg == 3){
        NDP.begin(fw1);
        NDP.load(fw2);
        NDP.load(fw3);
        NDP.getInfo();
        ndp_is_init = true;

#ifdef WITH_IMU
        NDP.configureInferenceThreshold(EI_CLASSIFIER_NN_INPUT_FRAME_SIZE);
#else   
        NDP.turnOnMicrophone();     
        NDP.getAudioChunkSize();    
#endif
        NDP.interrupts();

        ei_syntiant_set_match();
        nicla::leds.setColor(off);
    }
    else{
        ei_printf("NDP not properly initialized\n");
        nicla::leds.setColor(red);
    }
    
    dev->get_ext_flash()->init_fs();   

    at = ei_at_init(dev);

    if (ndp_is_init == true) {
        /* sensor init */
        ei_inertial_init();
        ei_run_nn_normal();
        setup2();
    }    
    
    ei_printf("Type AT+HELP to see a list of commands.\r\n");
    at->print_prompt();
}

/**
 * @brief 
 * 
 */
void bmi_main(){
  static unsigned long last_imu_read_ms = 0;
  unsigned long now = millis();

  if ((now - last_imu_read_ms) < 100) {
    return;
  }
  last_imu_read_ms = now;

  uint8_t __attribute__((aligned(4))) sensor_data[SENSOR_DATA_LENGTH];

  int16_t x_acc_raw, y_acc_raw, z_acc_raw, x_gyr_raw, y_gyr_raw, z_gyr_raw;
  float x_acc, y_acc, z_acc, x_gyr, y_gyr, z_gyr;

  int status;

  status = NDP.sensorBMI270Read(READ_START_ADDRESS, READ_BYTE_COUNT, &sensor_data[0]);

  CHECK_STATUS(status);

  x_acc_raw = (0x0000 | sensor_data[0] | sensor_data[1] << 8);
  y_acc_raw = (0x0000 | sensor_data[2] | sensor_data[3] << 8);
  z_acc_raw = (0x0000 | sensor_data[4] | sensor_data[5] << 8);
  x_gyr_raw = (0x0000 | sensor_data[6] | sensor_data[7] << 8);
  y_gyr_raw = (0x0000 | sensor_data[8] | sensor_data[9] << 8);
  z_gyr_raw = (0x0000 | sensor_data[10] | sensor_data[11] << 8);

  x_acc = x_acc_raw * ACCEL_SCALE_FACTOR;
  y_acc = y_acc_raw * ACCEL_SCALE_FACTOR;
  z_acc = z_acc_raw * ACCEL_SCALE_FACTOR;

  x_gyr = x_gyr_raw * GYRO_SCALE_FACTOR;
  y_gyr = y_gyr_raw * GYRO_SCALE_FACTOR;
  z_gyr = z_gyr_raw * GYRO_SCALE_FACTOR;

  static constexpr int FILTER_SAMPLES = 8;
  static float x_hist[FILTER_SAMPLES] = {0};
  static float y_hist[FILTER_SAMPLES] = {0};
  static float z_hist[FILTER_SAMPLES] = {0};
  static int hist_index = 0;
  static bool hist_full = false;

  x_hist[hist_index] = x_acc;
  y_hist[hist_index] = y_acc;
  z_hist[hist_index] = z_acc;

  hist_index++;
  if (hist_index >= FILTER_SAMPLES) {
    hist_index = 0;
    hist_full = true;
  }

  int valid_samples = hist_full ? FILTER_SAMPLES : hist_index;
  if (valid_samples <= 0) {
    return;
  }

  float x_sum = 0.0f;
  float y_sum = 0.0f;
  float z_sum = 0.0f;

  for (int i = 0; i < valid_samples; i++) {
    x_sum += x_hist[i];
    y_sum += y_hist[i];
    z_sum += z_hist[i];
  }

  float x_acc_f = x_sum / valid_samples;
  float y_acc_f = y_sum / valid_samples;
  float z_acc_f = z_sum / valid_samples;

  float magnitude = sqrt(x_acc_f * x_acc_f + y_acc_f * y_acc_f + z_acc_f * z_acc_f);
  float gyro_abs_max = max(max(fabs(x_gyr), fabs(y_gyr)), fabs(z_gyr));

  static bool supina = false;
	  static bool last_reported_supina = false;
	  static bool has_reported_supina = false;
	  static unsigned long enter_candidate_since = 0;
	  static unsigned long exit_candidate_since = 0;

  const float GRAVITY_MIN = 9.2f;
  const float GRAVITY_MAX = 10.3f;
  const float GYRO_MAX = 12.0f;

  const float ENTER_Z_MIN = 8.8f;
  const float ENTER_X_MAX = 2.0f;
  const float ENTER_Y_MAX = 3.0f;

  const float EXIT_Z_MIN = 8.3f;
  const float EXIT_X_MAX = 2.4f;
  const float EXIT_Y_MAX = 3.4f;

  const float DOMINANCE_MARGIN = 6.0f;

  const unsigned long ENTER_TIME_MS = 1200;
  const unsigned long EXIT_TIME_MS = 800;

  bool stable_gravity = (magnitude >= GRAVITY_MIN && magnitude <= GRAVITY_MAX);
  bool low_rotation = (gyro_abs_max <= GYRO_MAX);

  bool enter_supina_candidate =
      stable_gravity &&
      low_rotation &&
      z_acc_f >= ENTER_Z_MIN &&
      fabs(x_acc_f) <= ENTER_X_MAX &&
      fabs(y_acc_f) <= ENTER_Y_MAX &&
      z_acc_f > fabs(x_acc_f) + DOMINANCE_MARGIN &&
      z_acc_f > fabs(y_acc_f) + DOMINANCE_MARGIN;

  bool stay_supina_candidate =
      stable_gravity &&
      low_rotation &&
      z_acc_f >= EXIT_Z_MIN &&
      fabs(x_acc_f) <= EXIT_X_MAX &&
      fabs(y_acc_f) <= EXIT_Y_MAX;
  
  if (!supina) {
    if (enter_supina_candidate) {
      if (enter_candidate_since == 0) {
        enter_candidate_since = now;
      }

	      if ((now - enter_candidate_since) >= ENTER_TIME_MS) {
	        supina = true;
	        enter_candidate_since = 0;
	        exit_candidate_since = 0;
	        nicla::leds.setColor(red);
	        digitalWrite(10, HIGH);
	        instante_anterior = now;
            ultimo_alerta_supina = now;
	        record_event(EVENT_SUPINA_ENTER, now);
	      }
	    }
    else {
      enter_candidate_since = 0;
      nicla::leds.setColor(off);
    }
  }
	  else {
	    if (stay_supina_candidate) {
	      exit_candidate_since = 0;
	      nicla::leds.setColor(red);

          if (digitalRead(10) == LOW &&
              (now - ultimo_alerta_supina >= intervalo_alerta_supina)) {
            digitalWrite(10, HIGH);
            instante_anterior = now;
            ultimo_alerta_supina = now;
          }
	    }
	    else {
	      if (exit_candidate_since == 0) {
	        exit_candidate_since = now;
      }

	      if ((now - exit_candidate_since) >= EXIT_TIME_MS) {
	        supina = false;
	        exit_candidate_since = 0;
	        nicla::leds.setColor(off);
            ultimo_alerta_supina = 0;
	        record_event(EVENT_SUPINA_EXIT, now);
	      }
	    }
	  }

  if (digitalRead(10) == HIGH &&
    (now - instante_anterior >= tempo_motor)) {

    digitalWrite(10, LOW);
}
  if (!has_reported_supina || last_reported_supina != supina) {
    last_reported_supina = supina;
    has_reported_supina = true;
    Serial.print("ESTADO SUPINA: ");
    Serial.print(supina ? 1 : 0);
    Serial.print(" | x_acc_f:");
    Serial.print(x_acc_f);
    Serial.print(", y_acc_f:");
    Serial.print(y_acc_f);
    Serial.print(", z_acc_f:");
    Serial.print(z_acc_f);
    Serial.print(", mag:");
    Serial.print(magnitude);
    Serial.print(", gyr_max:");
    Serial.println(gyro_abs_max);
  }
}

void ei_main(void)
{
    int match = -1;

    while (Serial.available() > 0) {
        char data = (char)Serial.read();

        if (data == 'E' || data == 'e') {
            dump_ram_events();
        }
        else if (data == 'D' || data == 'd') {
            dump_csv_events();
        }
        else if (data == 'C' || data == 'c') {
            clear_events();
        }

        at->handle(data);

        if (ei_run_impulse_is_active() && data == 'b') {
            ei_start_stop_run_impulse(false);
        }
    }

    process_event_csv_flush(false);

    if (ei_run_impulse_is_active() == true) {
        if (got_match == true) {
            got_match = false;
            process_pending_match_events();
        }

        if (got_event == true) {
            got_event = false;
            nicla::leds.setColor(green);
            nicla::leds.setColor(off);
            match = NDP.poll();
        }

        if (match > 0) {
            ei_printf("match: %d\n", match);
        }

#ifdef WITH_IMU
        if (ei_run_impulse_is_active()) {
            ei_run_impulse();
        }
#endif
    }

    instante_atual = millis();
    if (instante_atual - instante_anterior >= tempo_motor) {
        instante_anterior = instante_atual;
        digitalWrite(10, LOW);
    }

    bmi_main();
}



/**
 * @brief disable interrupt from NDP class
 * 
 */
void ei_syntiant_clear_match(void)
{
    _on_match_enabled = false;
    //NDP.turnOffMicrophone();
    //NDP.noInterrupts();
}

/**
 * @brief enable interrupt from NDP clas
 * 
 */
void ei_syntiant_set_match(void)
{
    _on_match_enabled = true;
    //NDP.turnOnMicrophone();
    //NDP.interrupts();
}

/**
 * @brief Callback when an Error is triggered
 * @note it never exit!
 */
static void error_event(void)
{
    nicla::leds.begin();
    while (1) {
        nicla::leds.setColor(red);
        ThisThread::sleep_for(250);
        nicla::leds.setColor(off);
        ThisThread::sleep_for(250);
    }
    nicla::leds.end();
}
void limpa_fila(){
    snore_event_head = 0;
    snore_event_count = 0;
}
/**
 * @brief Callback when a Match is triggered
 * 
 * @param label The Match label
 */
static void match_event(char* label)
{

    if (_on_match_enabled == true) {
        if (strlen(label) > 0) {
            got_match = true;
            enqueue_pending_match_event(label, millis());
        }
    }
}

/**
 * @brief 
 * 
 */
static void irq_event(void)
{    
    if (_on_match_enabled == true) {
        got_event = true;
    }
}

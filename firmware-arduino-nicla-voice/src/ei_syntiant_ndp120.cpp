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
#include <queue>
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
      Serial.println(__LINE__); \
      for (;;) \
        ; \
    } \
  } while (0)
#include "mbed.h"


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

static bool _on_match_enabled = false;
static volatile bool got_match = false;
static volatile bool got_event = false;

std::queue<unsigned long>fila; 
unsigned long instante_anterior=0; 
const unsigned long tempo_motor=2000; 
unsigned long instante_atual=0;
/* Public functions -------------------------------------------------------- */
/**
 * @brief 
 * 
 */
 unsigned long anteriormillis=0;

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
    }    
    setup2();
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

  Serial.print("x_acc:");
  Serial.print(x_acc);
  Serial.print(",");
  Serial.print("y_acc:");
  Serial.print(y_acc);
  Serial.print(",");
  Serial.print("z_acc:");
  Serial.println(z_acc);

  float magnitude = sqrt(x_acc_f * x_acc_f + y_acc_f * y_acc_f + z_acc_f * z_acc_f);
  float gyro_abs_max = max(max(fabs(x_gyr), fabs(y_gyr)), fabs(z_gyr));

  static bool supina = false;
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

// if (magnitude > shakeThreshold) {
//     Serial.println("Sacudida forte detectada!");

//     // Ação ao detectar sacudida:
//    NRF_WDT->TASKS_STOP = 1;
//    NRF_WDT->CONFIG = 0;
//       // Entra em modo desligado
//     NRF_POWER->RESETREAS = 0xFFFFFFFF;  // limpa causas de reset
//     NRF_POWER->SYSTEMOFF = 1;


//     // Opcional: modo sleep
//     delay(1000); // Curto delay para estabilizar
//     // sleep(); // ou deepSleep(); se quiser suspender o dispositivo
// }

//   Serial.print("magnitude: ");
//   Serial.println(magnitude);
//   // Print gyroscope data (expressed in °/s). 
  
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
      digitalWrite(10, HIGH);
      instante_anterior = now;
    }
    else {
      if (exit_candidate_since == 0) {
        exit_candidate_since = now;
      }

      if ((now - exit_candidate_since) >= EXIT_TIME_MS) {
        supina = false;
        exit_candidate_since = 0;
        nicla::leds.setColor(off);
      }
    }
  }

  if ((now - instante_anterior) >= tempo_motor) {
    digitalWrite(10, LOW);
  }

  Serial.print("x_acc_f:");
  Serial.print(x_acc_f);
  Serial.print(", y_acc_f:");
  Serial.print(y_acc_f);
  Serial.print(", z_acc_f:");
  Serial.print(z_acc_f);
  Serial.print(", mag:");
  Serial.print(magnitude);
  Serial.print(", gyr_max:");
  Serial.print(gyro_abs_max);
  Serial.print(", supina:");
  Serial.println(supina ? 1 : 0);
}


void ei_main(void)
{
    int match = -1;
    /* handle command comming from uart */
    char data = Serial.read();

    while (data != 0xFF) {
        at->handle(data);

        if (ei_run_impulse_is_active() && data == 'b') {
            ei_start_stop_run_impulse(false);
        } 
        
        data = Serial.read();
    

    if (ei_run_impulse_is_active() ==true) {
        if (got_match == true){
            got_match = false;
            nicla::leds.setColor(blue);
            
            ThisThread::sleep_for(100);
            //nicla::leds.setColor(off);
        }

        if (got_event == true){
            got_event = false;
            nicla::leds.setColor(green);
            ThisThread::sleep_for(100);
            nicla::leds.setColor(off);
            match = NDP.poll();
        }

        if (match > 0) {
            ei_printf("match: %d\n", match);
            match = -1;
        }

#ifdef WITH_IMU
        // for now by default we stay in inference
        if (ei_run_impulse_is_active()) {
            ei_run_impulse();
        }
#endif
    }
}

instante_atual=millis();
if (instante_atual -instante_anterior>=tempo_motor){
    instante_anterior=instante_atual; 
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
    while(!fila.empty()){
        fila.pop();
    }
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
             
                        if( 0==strcmp(label, "NN0:Snoring") )
	    {
           Serial.print("RONCO DETETADO EM: ");
           Serial.println(millis());
	       nicla::leds.setColor(blue);
           fila.push(millis());
           if (fila.size()>3){
                unsigned long dif= fila.back()- fila.front() ;
                if (dif>30000){
                    limpa_fila();
                } else 
                if (dif>6000){
                    digitalWrite(10, HIGH); 
                    instante_anterior=millis();
                } 
           }
           
            ThisThread::sleep_for(100);
               //nicla::leds.setColor(off);    
            }


	    else
	    {
	       nicla::leds.setColor(green);
               ThisThread::sleep_for(100);
               //nicla::leds.setColor(off);    
            }


            ei_printf("Match: %s\n", label);            
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

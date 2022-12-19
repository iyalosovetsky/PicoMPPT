# Complete project details at https://RandomNerdTutorials.com
import ntptime
import time
#from umqttsimple import MQTTClient
import ubinascii
import machine
import micropython
import network
#import esp
#esp.osdebug(None)
import gc
gc.collect()

ssid = 'IGORNET'
password = 'IG0RNET29041971'
mqtt_server = '10.80.39.78'
mqtt_user='igor'
mqtt_password='p29041971'
#EXAMPLE IP ADDRESS
#mqtt_server = '192.168.1.144'
client_id = ubinascii.hexlify(machine.unique_id())
topic_sub = b'house/pico/in'
topic_pub = b'house/pico/out'


mqtt_keepalive=7200


message_intervalPop = 4
message_intervalGS=120 #2 minute
error_cnt=0



station = network.WLAN(network.STA_IF)

station.active(True)
station.connect(ssid, password)

while station.isconnected() == False:
  pass
 
rtc=machine.RTC()
ntptime.settime() 
print(f'start UTC {rtc.datetime()[2]:02}.{rtc.datetime()[1]:02}.{rtc.datetime()[0]:04} {rtc.datetime()[4]:02}:{rtc.datetime()[5]:02}')

print('Connection successful',station.ifconfig())



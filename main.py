from machine import UART, Pin
import time

import time
from umqtt.robust import MQTTClient
import machine
import json











import pi18
prot=pi18.pi18()
uart0 = UART(0, baudrate=2400, parity=None, stop=1, tx=Pin(0), rx=Pin(1))

# message_intervalPop = 4
# message_intervalGS=121 #2 minute
# message_intervalED=301 #5 minute
# message_intervalNTP=3601 #1hour

counter = 0
# last_message = 0
# last_messageGS=0
# last_messageED=0
# last_messageNTP=0
error_cnt = 0
error_cnt_others = 0
debugmode = 0


def restart_and_reconnect():
  print('Too many errors. Reconnecting...')
  time.sleep(10)
  error_cnt = 0
  machine.reset()

def sendCmd0(connector, request):
    connector.write(request[:8])
    if len(request) > 8:
        connector.write(request[8:])


answer_uart = []
#uartr=b'^D1060000,000,2301,500,0437,0292,010,528,000,000,005,000,100,030,000,000,0000,0000,0000,0000,0,0,0,1,2,2,0,0p\xdc\r'    


    

def sub_cb2Uart(msg):
    global error_cnt
    try:
        if msg=='MAIN':
          cmd=prot.get_full_command('GS')
        elif msg.startswith('DS'):
          cmd=prot.get_full_command('ED'+msg[2:])
        else:
          cmd=prot.get_full_command(msg)
        print(cmd,"to uart0")
        if cmd is None:
            print("break for None cmd")
            #return
        uartr=""
        sendCmd0(uart0, cmd)
        time.sleep(0.75)
        while uart0.any() > 0:    #Channel 0 is spontaneous and self-collecting
            uartr=uart0.read()
        if debugmode >2:   
            if msg=='MAIN':    
                uartr=b'^D1060000,000,2301,500,0437,0292,010,528,000,000,005,000,100,030,000,000,0000,0000,0000,0000,0,0,0,1,2,2,0,0p\xdc\r'        
            elif msg=='GS':    
                uartr=b'^D1060000,000,2301,500,0437,0292,010,528,000,000,005,000,100,030,000,000,0000,0000,0000,0000,0,0,0,1,2,2,0,0p\xdc\r'        
        if uartr is None or uartr=='':
            if debugmode>0:
                publish(topic_pub, "uart return empty msg for %s"%msg)
            return 
        print("uart:",uartr)
        while len(answer_uart)>10:
                answer_uart.pop(0)
        answer_uart.append([msg,uartr,time.time()])
    except OSError as e:
        error_cnt +=1
        print('OSError error_cnt',error_cnt, e)
        if Exception>10:
              print('OSError restart_and_reconnect()')
              restart_and_reconnect()
    except Exception as e:
        print('Exception error_cnt',error_cnt, e)

def sub_cb(topic, msg0): 
    global error_cnt, debugmode 
    try:
          msg=  msg0.decode("utf-8")
          print(rtc.datetime())
          global testmode
          if topic == topic_sub and (msg in  prot.STATUS_COMMANDS) :
            if msg=='ED':
               msg= 'ED'+f'{rtc.datetime()[0]:04}{rtc.datetime()[1]:02}{rtc.datetime()[2]:02}'
               if debugmode>0:
                  publish(topic_pub, "change to %s"%msg)
            elif msg=='EM':
               msg= 'EM'+f'{rtc.datetime()[0]:04}{rtc.datetime()[1]:02}'
               if debugmode>0:
                  publish(topic_pub, "change to %s"%msg)
            elif msg=='EY':
               msg= 'EY'+f'{rtc.datetime()[0]:04}'   
               if debugmode>0:
                  publish(topic_pub, "change to %s"%msg)
            print('Pico received STATUS_COMMANDS',msg)
            sub_cb2Uart(msg)
          elif topic == topic_sub and (msg[0:2] in  prot.STATUS_COMMANDS) :
            print('Pico received STATUS_COMMANDS',msg)
            sub_cb2Uart(msg)
          elif topic == topic_sub and (msg in  prot.SETTINGS_COMMANDS) :
            print('Pico received SETTINGS_COMMANDS',msg)
            sub_cb2Uart(msg)
          elif topic == topic_sub and  msg=='MAIN':
            print('Pico received general query',msg)
            sub_cb2Uart(msg)
            #publish(topic_pub, cmd)
          elif topic == topic_sub and  msg=='DS':
            msg= 'DS'+f'{rtc.datetime()[0]:04}{rtc.datetime()[1]:02}{rtc.datetime()[2]:02}'
            if debugmode>0:
                  publish(topic_pub, "change to %s"%msg)
            print('Pico received dayly stats query',msg)
            sub_cb2Uart(msg)
          elif topic == topic_sub and  msg.startswith('DEBUG'):
            if msg!='DEBUG' : 
              debugmode=int(msg.replace('DEBUG',''))
            else:
              debugmode=1
            msgpub=f'Pico received DEBUG %d'%(debugmode,)
            publish(topic_pub, msgpub)
          elif topic == topic_sub :
            msgpub=f'Pico received unknown %s'%(msg,)              
            publish(topic_pub, msgpub)
          else :
            print('Pico received ???',topic, msg)
    except Exception as e:
        print('Exception error_cnt',error_cnt, e)

  

def connect_and_subscribe():
  global client_id, mqtt_server, mqtt_user,mqtt_password, mqtt_keepalive, topic_sub
  client = MQTTClient(client_id=client_id, port=1883,server=mqtt_server,user=mqtt_user, password=mqtt_password,keepalive=mqtt_keepalive)
  client.set_callback(sub_cb)
  client.connect()
  client.subscribe(topic_sub)
  print('Connected to %s MQTT broker, subscribed to %s topic' % (mqtt_server, topic_sub))
  return client


  
def publish(topic, value):
    #print(topic)
    #print(value)
    client.publish(topic, value)
    #print("publish Done")  
  
 

MAIN_KEYS=["AC output voltage",
                 "Inverter heat sink temperature",
                 "Battery voltage from SCC",
                 "PV1 Input power","AC output active power",
                 "Battery capacity",
                 "MPPT1 charger status",
                 "Battery voltage",
                 "DC/AC power direction",
                 "AC output apparent power",
                 "Battery charging current",
                 "Battery power direction",
                 "PV1 Input voltage"]

def filter_answer (answOne):
    res=[]
    if answOne[1] is None or answOne[1]=='':
        return res
    if answOne[0]=='MAIN':
        answer_full =prot.decode(answOne[1],"GS")
    elif answOne[0].startswith('DS'):
        answer_full =prot.decode(answOne[1],'ED'+answOne[0][2:])
    else:    
        answer_full =prot.decode(answOne[1],answOne[0])
    for i, k in enumerate(answer_full):
        if k=='_command':
            continue
        elif k=='_command_description':
            continue
        elif k=='raw_response':
            continue
        elif '2' in k or 'Grid' in k:
            continue
        elif 'parallel' in k:
            continue
        else:
            if answOne[0]=='MAIN':
                res.append([topic_pub+b'/'+k.replace(' ','_'),answer_full[k][0]])
            elif answOne[0].startswith('DS'):
                res.append([topic_pub+b'/'+k.replace(' ','_'),answer_full[k][0]])
            else:
                obj= {}
                obj[k]=answer_full[k]
                res.append(obj)
    return res

def process_pop_msg():
    try:
        ii=0  
        while len(answer_uart)>0 and ii<100:
          ii+=1
          answOne=answer_uart.pop(0)
          answLst=filter_answer (answOne)
          if answOne[0]=='MAIN':
              for answOne in answLst:
                  answ=json.dumps(answOne[1])
                  publish(answOne[0],answ)
          elif answOne[0].startswith('DS'):
              for answOne in answLst:
                  answ=json.dumps(answOne[1])
                  publish(answOne[0],answ)
          else:
              for answOne in answLst:
                  answ=json.dumps(answOne)
                  publish(topic_pub,answ)
    except Exception as e:
        print('process_pop_msg',e)
        return -6
    else:
        return 0


def process_get_state():
    global counter
    counter += 1
    try:
        now=f'UTC {rtc.datetime()[2]:02}.{rtc.datetime()[1]:02}.{rtc.datetime()[0]:04} {rtc.datetime()[4]:02}:{rtc.datetime()[5]:02}'
        msg = b'Hello %s #%d ip %s' % (now, counter, station.ifconfig()[0])
        publish(topic_pub, msg)
        publish(topic_sub, 'MAIN')
    except Exception as e:
        print('process_ed',e)
        return -4
    else:
        return 0
        

def process_ed():
    global counter
    counter += 1
    try:
        now=f'UTC {rtc.datetime()[2]:02}.{rtc.datetime()[1]:02}.{rtc.datetime()[0]:04} {rtc.datetime()[4]:02}:{rtc.datetime()[5]:02}'
        msg = b'I will get dayly stats %s #%d ip %s' % (now, counter, station.ifconfig()[0])
        publish(topic_pub, msg)
        publish(topic_sub, 'DS')
    except Exception as e:
        print('process_ed',e)
        return -3
    else:
        return 0
    

    

def process_show_error():
    try:
        msg = b'Try to reboot device #%d ip %s' % (counter, station.ifconfig()[0])
        publish(topic_pub, msg)
        return 0
    except Exception as e:
        print('process_show_error',e)
        return -2

def process_ntp():
    try:
        ntptime.settime() 
        print(f'start UTC {rtc.datetime()[2]:02}.{rtc.datetime()[1]:02}.{rtc.datetime()[0]:04} {rtc.datetime()[4]:02}:{rtc.datetime()[5]:02}')
        return 0
    except Exception as e:
        print('Exception to set ntptime',e)
        return -1

def process_in_msg():
    try:
      client.check_msg()
    except Exception as e:
        print('process_in_msg',e)
        return -2
    else:
        return 0
   
    
def process_mqtt_isconnected():
    
    try:
        client.ping()
        client.ping()
    except:
        print("\nlost connection to mqtt broker...")
        try:
            connect_and_subscribe()
            return 1
        except Exception as e:
            print('process_mqtt_isconnected',e)
            return -5
    else:
        return 0
    
  
if station.isconnected() == False:
    while station.isconnected() == False:
      pass

    print('Connection successful')
    
print(station.ifconfig())
print('----------')





try:
    print('try to connect to MQTT broker')
    client = connect_and_subscribe()
except OSError as e:
  restart_and_reconnect()

publish(topic_pub+b'/ip',station.ifconfig()[0])




rt={}
rt['MQTTIN'] = {'last_start': time.time (), 'interval': 0.5, 'proc': process_in_msg , 'last_error': -99}
rt['POP'] = {'last_start': time.time (), 'interval': 4, 'proc': process_pop_msg , 'last_error': -99}
rt['HELLO'] = {'last_start': time.time ()+270, 'interval': 301, 'proc': process_get_state , 'last_error': -99}
rt['ED'] = {'last_start': time.time ()+860, 'interval': 901, 'proc': process_ed , 'last_error': -99}
rt['NTP'] = {'last_start': time.time ()-3590, 'interval': 3601, 'proc': process_ntp , 'last_error': -99}
rt['HEALTH'] = {'last_start': time.time (), 'interval': 1801, 'proc': process_mqtt_isconnected , 'last_error': -99}

 
 

    

def p_RTLoop():
    for key, value in rt.items ():
        l_time = time.time ()
        l_timeprev = value['last_start']
        if value['proc'] is not None:
            if (((l_time - l_timeprev) > value['interval'])) or (((l_time - l_timeprev) < 0)) or value['last_error'] == -99:
                #print('try', key)
                try:
                    value['last_error'] = value['proc'] ()
                    if value['last_error'] is None:
                        value['last_error'] = 0
                    if value['last_error'] >= 0:
                        value['last_start'] = l_time
                    else:
                        print('error on ', key)
                except Exception as e:
                    print ('error rt ' + key, e)


while True:
    try:
        p_RTLoop()
    except OSError as e:
        error_cnt +=1
        print('OSError error_cnt',error_cnt, e)
        try:
            process_show_error()
        except Exception as e:
            print('cant broadcast error', e)
        if error_cnt>10:
            print('OSError restart_and_reconnect()')
            restart_and_reconnect()
    except Exception as e:
        error_cnt_others +=1
        print('Exception error_cnt',error_cnt_others, e)
        if error_cnt>100:
           print('Exception restart_and_reconnect()')
           restart_and_reconnect()
        
    # print('point200')

# while True:
#   try:
#     time.sleep(0.1)
#     client.check_msg()
#     if (time.time() - last_message) > message_intervalPop and answer_uart is not None and answer_uart!='':
#         last_message = time.time()
#         process_pop_msg()
#     if (time.time() - last_messageGS) > message_intervalGS:
#         last_messageGS = time.time()
#         process_get_state()
#     if (time.time() - last_messageED) > message_intervalED:
#         last_messageED = time.time()
#         process_ed()
#     if (time.time() - last_messageNTP) > message_intervalNTP:
#         last_messageNTP = time.time()
#         process_ntp()
#   except OSError as e:
#       error_cnt +=1
#       print('OSError error_cnt',error_cnt, e)
#       try:
#           process_show_error()
#       except Exception as e:
#           print('cant broadcast error', e)
#       if error_cnt>10:
#           print('OSError restart_and_reconnect()')
#           restart_and_reconnect()




#respt=b'^D1060000,000,2301,500,0437,0292,010,528,000,000,005,000,100,030,000,000,0000,0000,0000,0000,0,0,0,1,2,2,0,0p\xdc\r'
#qt=b'^P005GSX\x14\r'
#b'^P013ED20221218\xe1l\r' to uart0
#uart: b'^D01100001654\xa7\xa5\r'
#print(respt,qt)






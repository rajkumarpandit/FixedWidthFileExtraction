import configparser
from cryptography.fernet import Fernet
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import socket


def getKeyVal(p_key_name):
    #p_key_name='DB_USER'
    cp = configparser.ConfigParser()  
    cpFilePath = './conf/keystore.txt'
    #cp.read(open(r'keystore.txt'))
    cp.read(cpFilePath)
    try:
        retval=  cp.get('keys', p_key_name)
    except:
        retval=''
                
    return retval

"""for one time back end operation only
#--------------------------------------
#generate the key
key = Fernet.generate_key()
print(key)
#b'Py1Pkns7pEJcTNi4-pkfYUAJW2XZ_-lIaGRPY9gZbFA='
#write it to the .in file under conf
cipher_suite = Fernet(key)
ciphered_text = cipher_suite.encrypt(b'TreaS2018')
with open('C:/PythonProjects/FixedWidthFileParsing/src/conf/application.bin', 'wb') as file_object:  
    file_object.write(ciphered_text)
#--------------------------------------------------------------------------------------------------
#for prod
C:\Python27\Scripts>python
Python 3.7.0 (v3.7.0:1bf9cc5093, Jun 27 2018, 04:59:51) [MSC v.1914 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license" for more information.
>>> import configparser
>>> from cryptography.fernet import Fernet
>>> import smtplib
>>> from email.mime.multipart import MIMEMultipart
>>> from email.mime.text import MIMEText
>>> import socket
>>> key = Fernet.generate_key()
>>> print(key)
b'3Lsdyx7TM7DEO4TOD-m0sF0tFtQKdH66iZ9CAkbFNIk='
>>> cipher_suite = Fernet(key)
>>> ciphered_text = cipher_suite.encrypt(b'admstars9')
>>> with open('C:/PythonProjects/FixedWidthFileParsing/src/conf/application_p.bin', 'wb') as file_object:
...    file_object.write(ciphered_text)  #Please notice the spacing
...
100
>>>
#---------------------------------------------------------------------------------------------------------
"""

def getDbPwd(p_env):
    
    if  p_env =='PRD':
        key = b'3Lsdyx7TM7DEO4TOD-m0sF0tFtQKdH66iZ9CAkbFNIk='
        cipher_suite = Fernet(key)
        with open('./conf/application_p.bin', 'rb') as file_object:
            for line in file_object:
                encryptedpwd = line
        uncipher_text = (cipher_suite.decrypt(encryptedpwd))
        plain_text_encryptedpassword = bytes(uncipher_text).decode("utf-8") #convert to string
    else:        
        key = b'Py1Pkns7pEJcTNi4-pkfYUAJW2XZ_-lIaGRPY9gZbFA='
        cipher_suite = Fernet(key)
        with open('./conf/application.bin', 'rb') as file_object:
            for line in file_object:
                encryptedpwd = line
        uncipher_text = (cipher_suite.decrypt(encryptedpwd))
        plain_text_encryptedpassword = bytes(uncipher_text).decode("utf-8") #convert to string
    return plain_text_encryptedpassword

def sendEmail(p_subject, p_message):
    hostname =socket.gethostname()
    print("Sending email...")
    print('Host:    '+ hostname)
    from_id=getKeyVal('EMAIL_FROM_ID')
    print('From:    '+from_id)
    if getKeyVal(hostname).find('PRD')>0:
        to_id=getKeyVal('EMAIL_RECEPIENT_PROD')
    else:
        to_id=getKeyVal('EMAIL_RECEPIENT_TEST')
    print('To:      '+to_id)
    msg = MIMEMultipart('alternative')
    msg['Subject'] = p_subject
    msg['From'] = from_id
    msg['To'] = to_id
    msg.attach(MIMEText(p_message, 'html'))
    s = smtplib.SMTP(getKeyVal('SMTP_SERVER'))  
    s.sendmail(from_id, to_id, msg.as_string())
    s.quit() 
    
def getEnv():
    hostname =socket.gethostname()
    env = getKeyVal(hostname )
    if env.find('PRD')>0 or env.find('STG')>0 or env.find('DEV')>0:
        return env
    else:
        return 'LOCAL'
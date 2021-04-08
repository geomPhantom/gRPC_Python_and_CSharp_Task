import sys
import logging
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP


f = open('public_rsa_key.pem','r')
pubKey = RSA.import_key(f.read())

encryptor = PKCS1_OAEP.new(pubKey)
#logging.error(len(pubKey.encode()))
f_ = open('des_key.txt','rb')
encrypted = encryptor.encrypt(f_.read())

f.close()
f_.close()

f__ = open('encrypted_des_key','wb')
f__.write(encrypted)
f__.close()

print("")
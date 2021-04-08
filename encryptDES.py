import sys
import logging
from cryptography.fernet import Fernet


key = Fernet.generate_key()
#logging.error(sys.argv[1])
print(key.decode())
logging.error(key.decode())
print(Fernet(key).encrypt(('\"'.join(sys.argv[1:])).encode()).decode())

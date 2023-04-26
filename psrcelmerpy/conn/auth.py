import os

def get_auth():
    try:
        uid = os.environ['SOCKEYE_UID']
        pwd = os.environ['SOCKEYE_PWD']
        
        pass
    except Exception as e:
        print(e.args[0])
        raise
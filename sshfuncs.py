import paramiko
import json
import os
import time

class Sshclass:
    def __init__(self, ip:str, user:str, pw:str, keypath:str, servername:str):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ip = ip
        self.user = user
        self.pw = pw
        self.keypath = keypath
        self.servername = servername
         
    def push_to_server(self, data:list):
        with open("./query.json", "w", encoding="utf-8") as f:
            json.dump(json.dumps(data,ensure_ascii=False),f)
            f.close()

        self.ssh.connect(self.ip, port='22', username=self.user, password=self.pw, key_filename=self.keypath)

        sftp = self.ssh.open_sftp()
        sftp.put('./query.json', '/home/ec2-user/Crawling/query.json')
        sftp.put('./steam_crawler.py', '/home/ec2-user/Crawling/steam_crawler.py')
        
        stdin, stdout, stderr =self.ssh.exec_command('cd ./Crawling;nohup python3 /home/ec2-user/Crawling/steam_crawler.py 1>nohup.out 2>&1 &')
            
        sftp.close()    
        self.ssh.close()

    def check_status(self): #작성중
        logpath = f'./sshcache/{os.listdir("./log")[-1]}_{self.servername}'
        self.ssh.connect(self.ip, port='22', username=self.user, password=self.pw, key_filename=self.keypath)

        sftp = self.ssh.open_sftp()
        sftp.get(f'/home/ec2-user/Crawling/log/{os.listdir("./log")[-1]}', logpath)
        
        sftp.close()
        self.ssh.close()
        
        with open(logpath, "r", encoding="utf-8") as f:
            fl = f.read().splitlines()[-1]
            f.close()
        if "code 200" in fl :return True
        return False
        
        

    def pull_from_server(self):
        self.ssh.connect(self.ip, port='22', username=self.user, password=self.pw, key_filename=self.keypath)

        sftp = self.ssh.open_sftp()
        sftp.get('/home/ec2-user/Crawling/Steamrev_base.csv', f'./sshcache/Steamrev_base_{self.servername}.csv')
        sftp.get('/home/ec2-user/Crawling/Steamrev_temp.csv', f'./sshcache/Steamrev_temp_{self.servername}.csv')
        sftp.get('/home/ec2-user/Crawling/Steamrev_summary.csv', f'./sshcache/Steamrev_summary_{self.servername}.csv')
        sftp.get(f'/home/ec2-user/Crawling/log/{os.listdir("./log")[-1]}', f'./sshcache/{os.listdir("./log")[-1]}_{self.servername}')
        sftp.close()
        self.ssh.close()

    def first_setup(self):
        self.ssh.connect(self.ip, port='22', username=self.user, password=self.pw, key_filename=self.keypath)

        sftp = self.ssh.open_sftp()
        sftp.put('../Crawling/requirements.txt', '/home/ec2-user/Crawling/requirements.txt')
        sftp.close()
        stdin, stdout, stderr = self.ssh.exec_command('pip3 install -r ./Crawling/requirements.txt')
        if stderr:error()
        self.ssh.close()
        
def run(host:Sshclass, data:list):
    host.check_status()
    #host.push_to_server(data)
    #while True:
    #  time.sleep(10)
    #  if host.check_status():break
    host.pull_from_server()
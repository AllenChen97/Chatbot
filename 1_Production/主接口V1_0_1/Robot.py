import requests, json, urllib
import time, hmac, hashlib, base64

class Robot():
    def __init__(self, webhook="https://oapi.dingtalk.com/robot/send?access_token=a96ced2bd30bfe4ae6ef36f5263bf642a4af0a94c50310687c189191d06c9431",secret=""):
        self.__appkey = "dinggusun3wou6cp1eev"
        self.__appsecret = "iTYs60fL6G7s_7N5ac4e6wCgDY9_w2X-gWWdJEGYRR9kI3QTC095rGJcbv-J3xn0"
        self.access_token = self.get_token()

        self.webhook = webhook      # webhook
        self.secret = secret        # 秘钥
        self.get_url()              # 计算url

    def get_token(self):  # 获取token
        params = {"appkey": self.__appkey, "appsecret": self.__appsecret}
        response = requests.get("https://oapi.dingtalk.com/gettoken", params=params)
        return response.json().get("access_token")

    def get_url(self):  # 获得秘钥 配置url
        timestamp = int(round(time.time() * 1000))

        secret_enc = bytes(self.secret, encoding='utf-8')  # .encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, self.secret)
        string_to_sign_enc = bytes(string_to_sign, encoding='utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()

        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))

        self.url = self.webhook + "&timestamp=" + str(timestamp) + '&sign=' + sign

    def send_msg(self, msg):
        header = {"Content-Type": "application/json", "Charset": "UTF-8"}
        data = {"msgtype": 'text', 'text': {'content': msg}}
        sendData = json.dumps(data).encode("utf-8")  # 对请求的数据进行json封装

        request = urllib.request.Request(url=self.url, data=sendData, headers=header)  # 发送请求
        opener = urllib.request.urlopen(request, timeout=10)  # 将请求发回的数据构建成为文件格式
        return json.load(opener)

    def upload_media(self, file_path):
        header = {"Content-Type": "application/json", "Charset": "UTF-8"}
        upload_url = 'https://oapi.dingtalk.com/media/upload?access_token=' + self.access_token + '&type=image'
        # 构建data字典(请求数据)
        files = {'media': open(file_path, 'rb')}
        data = {'access_token': self.access_token, 'type': 'image'}
        # 向带有access_token的url发送post请求，携带data和file参数
        response = requests.post(upload_url, files=files, data=data)
        return response.json()['media_id']

    def send_markdown(self, content):  # 发送markdown
        header = {"Content-Type": "application/json", "Charset": "UTF-8"}
        # 构建data字典(请求数据)，发送post请求
        data = {"msgtype": 'markdown', 'markdown': content}
        sendData = json.dumps(data).encode("utf-8")  # 对请求的数据进行json封装
        # 向带有access_token的url发送post请求，携带data和file参数
        request = urllib.request.Request(url=self.url, data=sendData, headers=header)  # 发送请求
        opener = urllib.request.urlopen(request, timeout=10)  # 将请求发回的数据构建成为文件格式
        return json.load(opener)
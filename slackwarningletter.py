from dotenv import  load_dotenv
from slack_sdk import WebClient
import os

class slack_warning:
    def __init__(self):
        load_dotenv()
        self.client = WebClient(token=os.getenv('SLACK_TOKEN'))
        self.channel = os.getenv('channel')
        self.channel_test = os.getenv('channel_test')
    def send_letter(self,text):
        self.client.chat_postMessage(channel=self.channel,text=text)
    def send_letter_test(self,text):
        self.client.chat_postMessage(channel=self.channel_test,text=text)

if __name__ == '__main__':
    slack_letter = slack_warning()
    slack_letter.send_letter('123')
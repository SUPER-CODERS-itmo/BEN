import sys
import os

root_path = os.path.abspath(os.path.dirname(__file__))
bot_path = os.path.join(root_path, 'bot')

if root_path not in sys.path:
    sys.path.insert(0, root_path)
if bot_path not in sys.path:
    sys.path.insert(0, bot_path)
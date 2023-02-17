#! /bin/bash
python3.8 background.py
gunicorn -w 4 -b :80 application:server

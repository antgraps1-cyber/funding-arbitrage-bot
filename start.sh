#!/bin/bash
python -u funding_bot.py > funding.log 2>&1 &
python -u price_bot.py > price.log 2>&1 &
gunicorn dashboard:app -b 0.0.0.0:$PORT

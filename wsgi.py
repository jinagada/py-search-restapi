# wsgi.py # app.py와 같은 위치
from app import app

if __name__ == '__main__':
    app.run()
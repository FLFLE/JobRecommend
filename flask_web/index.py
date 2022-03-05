from flask import render_template
from flask import request
from flask import Flask
from user_portrait import user_protrait_tag
from user_portrait.user_protrait_tag import User

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/search', methods=['POST'])
def search_user():
    user_id = request.form.get('user_id')
    print(user_id)
    tags = User(user_id).get_protrait()
    print(tags)
    return render_template('user_portrait.html')


if __name__ == '__main__':
    app.run()

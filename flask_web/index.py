from flask import render_template
from flask import request
from flask import Flask
from user_portrait import user_protrait_tag
from user_portrait.user_protrait_tag import User
import json

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/search', methods=['POST'])
def search_user():
    user_id = request.form.get('user_id')
    tags = User(user_id).get_protrait()
    print(tags)
    schema = ['user_sex', 'user_position_type', 'user_age', 'user_education', 'user_expectposition', 'user_expectcity',
              'user_expectsalarys', 'user_status', 'user_latest_schoolname', 'user_latest_deliver']
    tags_json = json.dumps(dict(zip(schema, list(tags))))
    print(tags_json)
    return render_template('user_portrait.html')


if __name__ == '__main__':
    app.run()

from flask import render_template
from flask import request
from flask import Flask
from user_portrait.user_protrait_tag import User
from position_portrait.position_protrait_tag import Position
import json

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/search_user', methods=['POST'])
def search_user():
    user_id = request.form.get('user_id')
    tags = User(user_id).get_protrait()
    schema = ['user_sex', 'user_position_type', 'user_age', 'user_education', 'user_expectposition', 'user_expectcity',
              'user_expectsalarys', 'user_status', 'user_latest_schoolname', 'user_latest_deliver']
    user_dict = dict(zip(schema, list(tags)))
    user_tags_json = json.dumps(user_dict, ensure_ascii=False)
    return render_template('user_portrait.html', user_tags_json=json.loads(user_tags_json))


@app.route('/search_position', methods=['POST'])
def search_position():
    position_id = request.form.get('position_id')
    tags = Position(position_id).get_position_protrait()
    schema = ['position_city', 'position_salary', 'position_workyear', 'position_category', 'position_education',
              'position_keywords']
    position_dict = dict(zip(schema, list(tags)))
    position_tags_json = json.dumps(position_dict, ensure_ascii=False)
    return render_template('position_protrait.html', position_tags_json=json.loads(position_tags_json))


if __name__ == '__main__':
    app.run()

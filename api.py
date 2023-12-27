import re
from flask import Flask, request,json, jsonify
from flask_swagger import swagger
from flask.views import MethodView
from utils  import Utils
from dq_utils import DQUtils

app = Flask("Lottabyte DQ API") 

class check_uniqueness_for_column(MethodView):
    #@app.route('/check_uniqueness_for_column', methods=['POST'])
    def post(self):
        """
        Проверка уникальности
        ---
        tags:
        - dq
        parameters:
          - in: body
            name: body
            schema:
              id: DQcheck
              required:
                - url
                - table
                - table_column
                - tenant
              properties:
                url:
                  type: string
                  description: url
                table:
                  type: string
                  description: table
                table_column:
                  type: string
                  description: table_column
                tenant:
                  type: string
                  description: tenant
        responses:
          200:
            description: Описание ошибки
            """
        req = json.loads(request.data)
        url = req['url']
        table = req['table']
        table_column = req['table_column']
        tenant = req['tenant']
        res =  DQUtils(Utils(tenant)).check_uniqueness_for_column(url,table, table_column)
        return {"res":res}
    
class check_null_values_for_column(MethodView):
    #@app.route('/check_null_values_for_column', methods=['POST'])
    def post(self):
        """
        Проверка на пустые значения
        ---
        tags:
        - dq
        parameters:
          - in: body
            name: body
            schema:
              id: DQcheck
             
        responses:
          200:
            description: Описание ошибки
            """
        req = json.loads(request.data)
        url = req['url']
        table = req['table']
        table_column = req['table_column']
        tenant = req['tenant']
        res =  DQUtils(Utils(tenant)).check_null_values_for_column(url,table, table_column)
        return {"res":res}
    
class check_range_for_column(MethodView):
    #@app.route('/check_range_for_column', methods=['POST'])
    def post(self):
        """
        Проверка диапазона значений
        ---
        tags:
        - dq
        parameters:
          - in: body
            name: body
            schema:
              id: DQcheck
             
        responses:
          200:
            description: Описание ошибки
            """
        req = json.loads(request.data)
        url = req['url']
        table = req['table']
        table_column = req['table_column']
        table_column_range = req['table_column_range']
        tenant = req['tenant']
        res =  DQUtils(Utils(tenant)).check_range_for_column(url,table, table_column,table_column_range)
        return {"res":res}

class check_regex(MethodView):
    def post(self):
      """
        Проверка regex
        ---
        tags:
        - dq
        parameters:
          - in: body
            name: body
            schema:
              id: DQcheck
              required:
                - value
                - regex
              properties:
                value:
                  type: string
                  description: value
                regex:
                  type: string
                  description: regex
             
        responses:
          200:
            description: Описание ошибки
            """

      req = json.loads(request.data)
      val = req['value']
      regex = req['regex']
      
      return {"res":True if re.search(regex, val) else False}



app.add_url_rule('/check_regex', view_func=check_regex.as_view('check_regex'), methods=['POST'])
app.add_url_rule('/check_uniqueness_for_column', view_func=check_uniqueness_for_column.as_view('check_uniqueness_for_column'), methods=['POST'])
app.add_url_rule('/check_null_values_for_column', view_func=check_null_values_for_column.as_view('check_null_values_for_column'), methods=['POST'])
app.add_url_rule('/check_range_for_column', view_func=check_range_for_column.as_view('check_range_for_column'), methods=['POST'])

@app.route("/spec")
def spec():
    swag = swagger(app)
    swag['info']['version'] = "1.0"
    swag['info']['title'] = "Lottabyte DQ API"
    response = jsonify(swag)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)

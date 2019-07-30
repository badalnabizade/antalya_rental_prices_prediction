import flask, pickle, os, json, requests
import pandas as pd
import numpy as np
from flask import Flask, render_template, request


county_list = pd.read_pickle('county_list.pickle')
district_list = pd.read_pickle('district_list.pickle')
with open('model_lgb.pkl','rb') as f:
            model_lgb = pickle.load(f)
model_features = pd.read_pickle('model_features.pickle')
data = pd.read_csv('data.csv')
    
    
app = Flask(__name__)
@app.route('/')
@app.route('/index')
def home():
    return flask.render_template('index.html')

@app.route('/score', methods=['POST', 'GET'])
def score():
    if request.method == 'POST':
        # get result from form and treat it
        input_json = request.form
        county = input_json['county']
        district = input_json['district']
        furnitured = int(input_json['furnitured'])
        residential_site = int(input_json['residential site'])
        heating_system = int(input_json['heating_system'])
        number_rooms = int(input_json['number_rooms'])
        number_bathroom = int(input_json['number_bathroom'])
        building_age = int(input_json['building_age'])
        floor_wthn_bldng = int(input_json['floor_wthn_bldng'])
        is_private = int(input_json['is_private'])
        has_view = int(input_json['has_view'])
        is_lux = int(input_json['is_lux'])
        near_uni = int(input_json['near_uni'])
        net_area = float(input_json['net_area'])

        def district_mean_price(data, district):
            """
            Returns mean price by given district.
            """
            return round(data[data['district'] == district]['price'].mean())

        #district that taken from user input is like 'in_[district]'. district[3:] removes 'in_' from input.
        mean_price = district_mean_price(data, district[3:])

        dct = {i:0 for i in model_features}
        dct[county] = 1
        dct[district] = 1
        dct['furnitured'] = furnitured
        dct['residential site'] = residential_site
        dct['heating_system'] = heating_system
        dct['number_rooms'] = number_rooms
        dct['number_bathroom'] = number_bathroom
        dct['building_age'] = building_age
        dct['floor_wthn_bldng'] = floor_wthn_bldng
        dct['is_private'] = is_private
        dct['has_view'] = has_view
        dct['is_lux'] = is_lux
        dct['near_uni'] = near_uni
        dct['net_area'] = net_area

        if net_area >= 18 and net_area < 27:
            dct['F1'] = 1
        elif net_area >= 27 and net_area < 41:
            dct['F2'] = 1
        elif net_area >=41 and net_area < 54:
            dct['F3'] = 1
        elif net_area >=54 and net_area < 66:
            dct['F4'] = 1
        elif net_area >= 66 and net_area < 79:
            dct['F5'] = 1
        elif net_area >= 79 and net_area < 89:
            dct['F6'] = 1
        elif net_area >= 89:
            dct['F7'] = 1

        df = pd.DataFrame(dct, index=[0])
        # Predictions from model are log-transformed. np.exp reverses log-transform.
        prediction = int(round(np.exp(model_lgb.predict(df.values))[0]))

        # render the html template sending the variables
        return render_template("result.html", price=str(prediction)+' TL',
                               district = district[3:],
                               mean_price = str(int(mean_price))+ ' TL')

if __name__ == '__main__':
    app.run(debug=True)

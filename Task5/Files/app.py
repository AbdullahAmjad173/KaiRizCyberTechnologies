from flask import Flask, request, jsonify
import pickle
import numpy as np

app = Flask(__name__)

# Load the trained model
model_path = 'D:/task5/model.pkl'
with open(model_path, 'rb') as file:
    model = pickle.load(file)

@app.route('/predict', methods=['POST'])
def predict():
    # Get the data from the POST request
    data = request.get_json(force=True)
    features = np.array(data['features']).reshape(1, -1)

    # Predict the price
    prediction = model.predict(features)
    
    # Return the result as JSON
    return jsonify({'price': prediction[0]})

if __name__ == '__main__':
    app.run(debug=True)

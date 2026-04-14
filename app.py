from flask import Flask, request, jsonify, render_template
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///crm.db'
db = SQLAlchemy(app)

class Customer(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(100), unique=True, nullable=False)
    phone = db.Column(db.String(20), nullable=True)

    def __repr__(self):
        return f'<Customer {self.name}>'

@app.route('/customers', methods=['GET'])
def get_customers():
    customers = Customer.query.all()
    return jsonify([{'id': c.id, 'name': c.name, 'email': c.email, 'phone': c.phone} for c in customers])

@app.route('/customer', methods=['POST'])
def add_customer():
    data = request.get_json()
    new_customer = Customer(name=data['name'], email=data['email'], phone=data.get('phone'))
    db.session.add(new_customer)
    db.session.commit()
    return jsonify({'message': 'Customer added successfully'}), 201

@app.route('/customer/<int:id>', methods=['PUT'])
def update_customer(id):
    data = request.get_json()
    customer = Customer.query.get_or_404(id)
    customer.name = data['name']
    customer.email = data['email']
    customer.phone = data.get('phone')
    db.session.commit()
    return jsonify({'message': 'Customer updated successfully'})

@app.route('/customer/<int:id>', methods=['DELETE'])
def delete_customer(id):
    customer = Customer.query.get_or_404(id)
    db.session.delete(customer)
    db.session.commit()
    return jsonify({'message': 'Customer deleted successfully'})

if __name__ == '__main__':
    db.create_all()
    app.run(debug=True)
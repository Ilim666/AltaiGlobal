from flask import Flask, render_template, request, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
import os

app = Flask(__name__)

# Конфигурация БД
basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'altai.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Модель Клиент
class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    phone = db.Column(db.String(20), nullable=False)
    address = db.Column(db.String(200), nullable=False)

    def __repr__(self):
        return f'<Client {self.name}>'

# Маршруты
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/clients', methods=['GET', 'POST'])
def clients():
    if request.method == 'POST':
        name = request.form.get('name')
        phone = request.form.get('phone')
        address = request.form.get('address')
        
        if name and phone and address:
            new_client = Client(name=name, phone=phone, address=address)
            db.session.add(new_client)
            db.session.commit()
            return redirect(url_for('clients'))
    
    all_clients = Client.query.all()
    return render_template('clients.html', clients=all_clients)

@app.route('/client/delete/<int:id>')
def delete_client(id):
    client = Client.query.get(id)
    if client:
        db.session.delete(client)
        db.session.commit()
    return redirect(url_for('clients'))

@app.route('/client/edit/<int:id>', methods=['GET', 'POST'])
def edit_client(id):
    client = Client.query.get(id)
    if request.method == 'POST':
        client.name = request.form.get('name')
        client.phone = request.form.get('phone')
        client.address = request.form.get('address')
        db.session.commit()
        return redirect(url_for('clients'))
    
    return render_template('edit_client.html', client=client)

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)
from flask import Flask, request, render_template, redirect, url_for
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///altai.db'
db = SQLAlchemy(app)

class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    phone = db.Column(db.String(20), nullable=False)
    address = db.Column(db.String(255), nullable=False)

    def __repr__(self):
        return f'<Client {self.name}>'


@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')


@app.route('/clients', methods=['GET', 'POST'])
def clients():
    if request.method == 'POST':
        new_client = Client(
            name=request.form['name'],
            phone=request.form['phone'],
            address=request.form['address']
        )
        db.session.add(new_client)
        db.session.commit()
        return redirect(url_for('clients'))

    all_clients = Client.query.all()
    return render_template('clients.html', clients=all_clients)


@app.route('/client/edit/<int:id>', methods=['GET', 'POST'])
def edit_client(id):
    client = Client.query.get_or_404(id)

    if request.method == 'POST':
        client.name = request.form['name']
        client.phone = request.form['phone']
        client.address = request.form['address']
        db.session.commit()
        return redirect(url_for('clients'))

    return render_template('edit_client.html', client=client)


@app.route('/client/delete/<int:id>', methods=['GET'])
def delete_client(id):
    client = Client.query.get_or_404(id)
    db.session.delete(client)
    db.session.commit()
    return redirect(url_for('clients'))

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)

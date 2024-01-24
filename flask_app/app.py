from datetime import datetime
from http import HTTPStatus
from urllib.parse import urlparse

from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:postgres@db/postgres'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class VisitedLink(db.Model):
    __tablename__ = 'visited_links'

    id = db.Column(db.Integer, primary_key=True)
    link = db.Column(db.String(255), nullable=False)
    visited_at = db.Column(db.DateTime, default=datetime.utcnow)


with app.app_context():
    db.create_all()


@app.route('/visited_links', methods=['POST'])
def add_visited_links():
    try:
        data = request.get_json()
        links = data.get('links', [])

        if len(links) == 0:
            return jsonify({"status": "Empty list"}), HTTPStatus.BAD_REQUEST
        else:
            with app.app_context():
                for link in links:
                    visited_link = VisitedLink(link=link)
                    db.session.add(visited_link)
                db.session.commit()
            return jsonify({"status": "ok"}), HTTPStatus.OK

    except Exception as e:
        return jsonify({"status": str(e)}), HTTPStatus.INTERNAL_SERVER_ERROR


@app.route('/visited_domains', methods=['GET'])
def get_visited_domains():
    try:
        if request.args.get('from') is None or request.args.get('to') is None:
            return jsonify({"status": "Parameters 'from' and 'to' are required"}), HTTPStatus.BAD_REQUEST
        try:
            from_timestamp = int(request.args.get('from'))
            to_timestamp = int(request.args.get('to'))
        except ValueError:
            return jsonify({"status": "Values of parameters must be timestamps"}), HTTPStatus.BAD_REQUEST

        if from_timestamp > to_timestamp:
            return jsonify({"status": "Parameter 'from' is greater than parameter 'to'"}), HTTPStatus.BAD_REQUEST

        else:
            from_datetime = datetime.utcfromtimestamp(from_timestamp)
            to_datetime = datetime.utcfromtimestamp(to_timestamp)

            result = db.session.query(VisitedLink.link).filter(
                VisitedLink.visited_at.between(from_datetime, to_datetime)
            ).distinct().all()

            domains = [urlparse(row[0]).netloc[4:] if urlparse(row[0]).netloc[:4] == "www."
                       else urlparse(row[0]).netloc for row in result]

            unique_domains = list(set(domains))

            return jsonify({"domains": unique_domains, "status": "ok"}), HTTPStatus.OK

    except Exception as e:
        return jsonify({"status": str(e)}), HTTPStatus.INTERNAL_SERVER_ERROR

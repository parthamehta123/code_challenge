from flask import Flask, jsonify, request, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, cast, Integer

app = Flask(__name__)
app.jinja_env.auto_reload = True
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////Users/parthamehta/python-workspace/code_challenge/db.sqlite3'
db = SQLAlchemy(app)


class WeatherData(db.Model):
    __tablename__ = 'weather_weatherdata'
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.String(8), nullable=False)
    station_id = db.Column(db.String(10), nullable=False)
    max_temp = db.Column(db.Float)
    min_temp = db.Column(db.Float)
    precipitation = db.Column(db.Float)


class WeatherDataStats(db.Model):
    __tablename__ = 'weather_statistics'
    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.String(4), nullable=False)
    station_id = db.Column(db.String(10), nullable=False)
    avg_max_temp = db.Column(db.Float)
    avg_min_temp = db.Column(db.Float)
    total_precipitation = db.Column(db.Float)


@app.route('/api/weather', methods=['GET'])
def get_weather():
    # Parse query parameters
    page = request.args.get('page', default=1, type=int)
    per_page = request.args.get('per_page', default=10, type=int)
    date = request.args.get('date')
    station_id = request.args.get('station_id')
    # Build query
    query = WeatherData.query
    if date:
        query = query.filter(WeatherData.date == date)
    if station_id:
        query = query.filter(WeatherData.station_id == station_id)
    # Paginate results
    pagination = query.paginate(page=page, per_page=per_page)
    # Build response
    data = [{'date': w.date, 'station_id': w.station_id,
             'max_temp': w.max_temp, 'min_temp': w.min_temp, 'precipitation': w.precipitation} for w in
            pagination.items]

    # Add pagination links and metadata
    prev_page = None
    next_page = None
    if pagination.has_prev:
        prev_page = url_for('get_weather', page=pagination.prev_num, _external=True)
    if pagination.has_next:
        next_page = url_for('get_weather', page=pagination.next_num, _external=True)
    if not next_page:
        next_page = "This is the last page. There are no more pages after this."
    if not prev_page:
        prev_page = "This is the first page. There are no pages before this."
    response = {
        "data": data,
        "total": pagination.total,
        "pages": pagination.pages,
        "next_page": next_page,
        "prev_page": prev_page
    }
    return jsonify(response)


# @app.route('/api/weather/stats', methods=['GET'])
# def get_weather_stats():
#     # Parse query parameters
#     year = request.args.get('year')
#     station_id = request.args.get('station_id')
#     page = int(request.args.get('page', 1))
#     per_page = 10 # Set the number of items per page
#     # Build query
#     query = db.session.query(
#         WeatherDataStats.station_id,
#         WeatherDataStats.year,
#         WeatherDataStats.avg_max_temp,
#         WeatherDataStats.avg_min_temp,
#         WeatherDataStats.total_precipitation
#     )
#     if year:
#         query = query.filter(WeatherDataStats.year == year)
#     if station_id:
#         query = query.filter(WeatherDataStats.station_id == station_id)
#     # Count total records
#     total_records = query.count()
#     # Apply pagination
#     query = query.limit(per_page).offset((page-1)*per_page)
#     # Execute query and build response
#     stats = {}
#     for row in query.all():
#         if row.station_id not in stats:
#             stats[row.station_id] = {}
#         stats[row.station_id][row.year] = {
#             'avg_max_temp': row.avg_max_temp,
#             'avg_min_temp': row.avg_min_temp,
#             'total_precipitation': row.total_precipitation
#         }
#     # Build pagination links
#     base_url = f"{request.url_root}api/weather/stats"
#     last_page = total_records // per_page + 1
#     pagination_links = {}
#     if page > 1:
#         pagination_links['prev_page'] = f"{base_url}?page={page-1}"
#     if page < last_page:
#         pagination_links['next_page'] = f"{base_url}?page={page+1}"
#     pagination_links['pages'] = last_page
#     pagination_links['total'] = total_records
#     # Build response dictionary
#     response = {
#         'stats': stats,
#         'pagination_links': pagination_links
#     }
#     return jsonify(response)

# @app.route('/api/weather/stats', methods=['GET'])
# def get_weather_stats():
#     # Parse query parameters
#     year = request.args.get('year')
#     station_id = request.args.get('station_id')
#     page = int(request.args.get('page', 1))
#     per_page = 10 # Set the number of items per page
#     # Build query
#     query = db.session.query(
#         WeatherDataStats.station_id,
#         WeatherDataStats.year,
#         WeatherDataStats.avg_max_temp,
#         WeatherDataStats.avg_min_temp,
#         WeatherDataStats.total_precipitation
#     )
#     if year:
#         query = query.filter(WeatherDataStats.year == year)
#     if station_id:
#         query = query.filter(WeatherDataStats.station_id == station_id)
#     # Count total records
#     total_records = query.count()
#     # Apply pagination
#     query = query.limit(per_page).offset((page-1)*per_page)
#     # Execute query and build response
#     stats = {}
#     for row in query.all():
#         if row.station_id not in stats:
#             stats[row.station_id] = {}
#         stats[row.station_id][row.year] = {
#             'avg_max_temp': row.avg_max_temp,
#             'avg_min_temp': row.avg_min_temp,
#             'total_precipitation': row.total_precipitation
#         }
#     # Build pagination links
#     base_url = f"{request.url_root}api/weather/stats"
#     last_page = total_records // per_page + 1
#     pagination_links = {}
#     if page > 1:
#         pagination_links['prev_page'] = f"{base_url}?page={page-1}"
#     else:
#         pagination_links['prev_page'] = "This is the first page. There are no pages before this."
#     if page < last_page:
#         pagination_links['next_page'] = f"{base_url}?page={page+1}"
#     else:
#         pagination_links['next_page'] = "This is the last page. There are no more pages after this."
#     pagination_links['pages'] = last_page
#     pagination_links['total'] = total_records
#     # Build response dictionary
#     response = {
#         'stats': stats,
#         'pagination_links': pagination_links
#     }
#     return jsonify(response)

# @app.route('/api/weather/stats', methods=['GET'])
# def get_weather_stats():
#     # Parse query parameters
#     year = request.args.get('year')
#     station_id = request.args.get('station_id')
#     page = int(request.args.get('page', 1))
#     per_page = 10 # Set the number of items per page
#     # Build query
#     query = db.session.query(
#         WeatherDataStats.station_id,
#         WeatherDataStats.year,
#         WeatherDataStats.avg_max_temp,
#         WeatherDataStats.avg_min_temp,
#         WeatherDataStats.total_precipitation
#     )
#     if year:
#         query = query.filter(WeatherDataStats.year == year)
#     if station_id:
#         query = query.filter(WeatherDataStats.station_id == station_id)
#     # Count total records
#     total_records = query.count()
#     # Apply pagination
#     query = query.limit(per_page).offset((page-1)*per_page)
#     # Execute query and build response
#     results = []
#     for row in query.all():
#         result = {
#             'station_id': row.station_id,
#             'year': row.year,
#             'avg_max_temp': row.avg_max_temp,
#             'avg_min_temp': row.avg_min_temp,
#             'total_precipitation': row.total_precipitation
#         }
#         results.append(result)
#     # Build pagination links
#     base_url = f"{request.url_root}api/weather/stats"
#     last_page = total_records // per_page + 1
#     pagination_links = {}
#     if page > 1:
#         pagination_links['prev_page'] = f"{base_url}?page={page-1}"
#     else:
#         pagination_links['prev_page'] = "This is the first page. There are no pages before this."
#     if page < last_page:
#         pagination_links['next_page'] = f"{base_url}?page={page+1}"
#     else:
#         pagination_links['next_page'] = "This is the last page. There are no more pages after this."
#     pagination_links['pages'] = last_page
#     pagination_links['total'] = total_records
#     # Build response dictionary
#     response = {
#         'results': results,
#         pagination_links
#     }
#     return jsonify(response)

@app.route('/api/weather/stats', methods=['GET'])
def get_weather_stats():
    # Parse query parameters
    year = request.args.get('year')
    station_id = request.args.get('station_id')
    page = int(request.args.get('page', 1))
    per_page = 10 # Set the number of items per page
    # Build query
    query = db.session.query(
        WeatherDataStats.station_id,
        WeatherDataStats.year,
        WeatherDataStats.avg_max_temp,
        WeatherDataStats.avg_min_temp,
        WeatherDataStats.total_precipitation
    )
    if year:
        query = query.filter(WeatherDataStats.year == year)
    if station_id:
        query = query.filter(WeatherDataStats.station_id == station_id)
    # Count total records
    total_records = query.count()
    # Apply pagination
    query = query.limit(per_page).offset((page-1)*per_page)
    # Execute query and build response
    results = []
    for row in query.all():
        result = {
            'station_id': row.station_id,
            'year': row.year,
            'avg_max_temp': row.avg_max_temp,
            'avg_min_temp': row.avg_min_temp,
            'total_precipitation': row.total_precipitation
        }
        results.append(result)
    # Build pagination links
    base_url = f"{request.url_root}api/weather/stats"
    last_page = total_records // per_page + 1
    pagination_links = {
        'next_page': f"{base_url}?page={page+1}" if page < last_page else None,
        'prev_page': f"{base_url}?page={page-1}" if page > 1 else None,
        'pages': last_page,
        'total': total_records
    }
    # Add checks for prev_page and next_page
    if pagination_links['prev_page'] is None:
        pagination_links['prev_page'] = "This is the first page. There are no pages before this."
    if pagination_links['next_page'] is None:
        pagination_links['next_page'] = "This is the last page. There are no more pages after this."
    # Build response dictionary
    response = {
        **pagination_links,
        'results': results
    }
    return jsonify(response)


if __name__ == '__main__':
    app.run(host='localhost', port=8000, debug=True)

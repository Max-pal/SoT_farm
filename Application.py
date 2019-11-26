from flask import Flask, render_template, url_for, request
from flask_charts import GoogleCharts
from flask_charts import Chart
import queries

charts = GoogleCharts()
app = Flask('__Flask__')
charts.init_app(app)


@app.route('/<day>')
def index(day):
    # headers = queries.get_days()
    # search = request.args.get('day')
    data = queries.get_tweets(day)
    data2 = queries.get_sources()
    my_chart = Chart("LineChart", "my_chart")
    my_chart.options = {
        "title": 'Tweets by hour'
    }
    my_chart.data.add_column("number", "Hour")
    my_chart.data.add_column("number", "Tweets")
    for tweet in data:
        my_chart.data.add_row([tweet['hour'], tweet['count']])
    my_piechart = Chart("PieChart", "my_piechart")
    my_piechart.options = {
        "title": "Client Sources",
        "is3D": True,
        "width": 500,
        "height": 500
    }
    my_piechart.data.add_column("string", "Client source")
    my_piechart.data.add_column("number", "Count")
    for tweet in data2:
        my_piechart.data.add_row([tweet['client_source'], tweet['count']])
    return render_template('index.html', my_chart=my_chart, my_piechart=my_piechart)


if __name__ == '__main__':
    app.run(host='35.156.176.186', port=80)

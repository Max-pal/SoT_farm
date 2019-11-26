import data_manager


# def get_tweets():
#     return data_manager.execute_select("""SELECT  COUNT(id_str), EXTRACT('day' FROM elonmusk.created_at)::int as day, EXTRACT('hour' FROM elonmusk.created_at)::int as hour, EXTRACT('minute' FROM elonmusk.created_at)::int as minute FROM elonmusk
# GROUP BY day, hour, minute;
# """)

def get_tweets(day):
    return data_manager.execute_select("""SELECT  COUNT(id_str), EXTRACT('hour' FROM elonmusk.created_at)::int as hour FROM elonmusk
GROUP BY EXTRACT('day' FROM elonmusk.created_at)::int, hour
HAVING EXTRACT('day' FROM elonmusk.created_at)::int = %(day)s
ORDER BY EXTRACT('hour' FROM elonmusk.created_at)::int ASC;""", {'day': day})

def get_sources():
    return data_manager.execute_select("""SELECT client_source, count(client_source) FROM elonmusk
GROUP BY client_source
HAVING count(client_source) > 85""")


def get_days():
    return data_manager.execute_select("""SELECT EXTRACT('day' FROM elonmusk.created_at) as day FROM elonmusk
GROUP BY day;""")
from settings import *
import views


def setup_routes(app):
    app.router.add_get('/', views.index)
    app.router.add_get(ROUTE_GET_LAST_ID, views.index, name='get_last_id', )

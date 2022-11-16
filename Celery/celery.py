from celery import Celery
from flask import Flask


# celery = Celery(__name__,
#                 broker="redis://127.0.0.1:6379",
#                 backend="redis://127.0.0.1:6379")
from flask_mail import Mail


def make_celery(app):
    celery = Celery(
        __name__,
        broker="redis://127.0.0.1:6379",
        backend="redis://127.0.0.1:6379"
    )
    celery.conf.update(app.config)
    #celery.autodiscover_tasks()

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery

app = Flask(__name__)
app.config.update(dict(
    DEBUG = True,
    MAIL_SERVER = 'smtp.office365.com',
    MAIL_PORT = 587,
    MAIL_USE_TLS = True,
    MAIL_USERNAME = "hydroclim_[testemail]@outlook.com",
    MAIL_PASSWORD = "[password]",
    MAIL_DEFAULT_SENDER = 'HydroClim <hydroclim_test@outlook.com>'
))
mail = Mail(app)
def create_app(config_name):
    app = Flask(__name__)

    return app


celery = make_celery(app)

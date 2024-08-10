from jinja2 import Environment, FileSystemLoader
import os, dotenv

dotenv.load_dotenv('/environment.env')
jinja_templates_path=os.getenv('JINJA_TEMPLATES_PATH')


def render_sql_template(template_file:str, **kwargs):
    env = Environment(loader=FileSystemLoader(jinja_templates_path))
    template = env.get_template(template_file)
    return template.render(**kwargs)

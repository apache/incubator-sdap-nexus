import sys
import traceback
import json


class NexusCAMLRenderer(object):
    def __init__(self, nexus_request):
        self.request = nexus_request

    def render(self, tornado_handler, result):
        tornado_handler.set_header("Content-Type", "application/json")
        try:
            result_str = result.toCAML(tornado_handler.request)
            tornado_handler.write(result_str)
            tornado_handler.finish()
        except AttributeError:
            traceback.print_exc(file=sys.stdout)
            tornado_handler.write(json.dumps(result, indent=4))
            tornado_handler.finish()

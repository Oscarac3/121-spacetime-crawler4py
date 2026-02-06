import pickle

class Response(object):
    '''
    Attributes:
        url:
            The URL identifying the response.
        status:
            An integer that identifies the status of the response. This
            follows the same status codes of http.
            (REF: https://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html)
            In addition there are status codes provided by the caching
            server (600-606) that define caching specific errors.
        error:
            If the status codes are between 600 and 606, the reason for
            the error is provided in this attribute. Note that for status codes
            (400-599), the error message is not put in this error attribute; instead it
            must picked up from the raw_response (if any, and if useful).
        raw_response:
            If the status is between 200-599 (standard http), the raw
            response object is the one defined by the requests library.
            Useful resources in understanding this raw response object:
                https://realpython.com/python-requests/#the-response
                https://requests.kennethreitz.org/en/master/api/#requests.Response
            HINT: raw_response.content gives you the webpage html content.
        '''

    def __init__(self, resp_dict):        
        self.url = resp_dict["url"]
        self.status = resp_dict["status"]
        self.error = resp_dict["error"] if "error" in resp_dict else None
        try:
            self.raw_response = (
                pickle.loads(resp_dict["response"])
                if "response" in resp_dict else
                None)
        except TypeError:
            self.raw_response = None

import unittest
import mock
import pycurl

from lib.__init__ import (fix_market_url, to_unicode, to_str, get_counters,
                            check_for_meta, prepare_url, make_pycurl_request,
                            get_url, get_redirect_history)

class Curl_fake:
    USERAGENT      = 'U'
    WRITEDATA      = 'W'
    FOLLOWLOCATION = 'F'
    URL            = 'U'
    TIMEOUT        = 'T'
    REDIRECT_URL   = None
    buffer         = None
    Text           = None

    def __init__(self, red, text):
        self.REDIRECT_URL = red
        self.Text = text

    def setopt(self, title, val):
        if title == self.WRITEDATA:
            self.buffer = val

    def perform(self):
        self.buffer.write(self.Text)

    def getinfo(self, title):
        return self.REDIRECT_URL

    def close(self):
        pass


def prepare_url_fake(url):
    return url


class InitCase(unittest.TestCase):
    def test_mack_pycurl_request(self):
        test_context = 'asdfasdfasdfasdfadsfasdfafdasdfasdfasdfasf'
        test_redirect_url = 'test_red'
        url = 'http://test.com'
        curl_mock = mock.Mock(return_value=Curl_fake(test_redirect_url, test_context))
        with mock.patch('pycurl.Curl', curl_mock):
            context, redirect_url = make_pycurl_request(url, 100, 'user_agent')
            curl_mock.assert_called_once_with()
            self.assertEqual(test_redirect_url, redirect_url)
            self.assertEqual(test_context, context)

    def test_mack_pycurl_request_none(self):
        test_context = 'aaaaaaaaaaaaasssssssssssssssss'
        test_redirect_url = None
        url = 'http://test.com'
        curl_mock = mock.Mock(return_value=Curl_fake(test_redirect_url, test_context))
        with mock.patch('pycurl.Curl', curl_mock):
            context, redirect_url = make_pycurl_request(url, 100, 'user_agent')
            curl_mock.assert_called_once_with()
            self.assertEqual(test_redirect_url, redirect_url)
            self.assertEqual(test_context, context)

    def test_fix_market_url(self):
        site = 'site.com'
        test_url = 'http://play.google.com/store/apps/' + site
        url = 'market://' + site
        self.assertEqual(test_url, fix_market_url(url))

    def test_to_unicode_StrToUnicode(self):
        val = 'test'
        self.assertEqual(True, isinstance(to_unicode(val, 'ignore'), unicode))

    def test_to_unicode_UnicodeToUnicode(self):
        val = 'test'.decode('utf-8', errors='ignore')
        self.assertEqual(True, isinstance(to_unicode(val, 'ignore'), unicode))

    def test_to_str_UnicoeToStr(self):
        val = 'test'.decode('utf-8', errors='ignore')
        self.assertEqual(True, isinstance(to_str(val, 'ignore'), basestring))

    def test_to_str_StrToStr(self):
        val = 'test'
        self.assertEqual(True, isinstance(to_str(val, 'ignore'), basestring))

    def test_get_counters(self):
        metric = 'YA_METRICA'
        content = '<!DOCTYPE html>' \
                  '<html><head>' \
                  '<script type="text/javascript" async="" src="https://mc.yandex.ru/metrika/watch.js"></script>' \
                  '</head><body></body></html>'
        self.assertEqual(metric, get_counters(content)[0])

    def test_check_for_meta_return_url(self):
        url_test = 'http://test.com'
        content = '<!DOCTYPE html>' \
                  '<html><head>' \
                  '<meta http-equiv="refresh" content="3000;url=' + url_test + '">' \
                  '</head><body></body></html>'
        self.assertEqual(url_test, check_for_meta(content, ''))

    def test_check_for_meta_return_none(self):
        url_test = 'http://test.com'
        content = '<!DOCTYPE html>' \
                  '<html><head>' \
                  '<meta http-equiv="refresh" content="url=' + url_test + '">' \
                  '</head><body></body></html>'

        self.assertEqual(None, check_for_meta(content, ''))

    def test_check_for_meta_not_return(self):
        url_test = 'http://test.com'
        content = '<!DOCTYPE html>' \
                  '<html><head>' \
                  '</head><body></body></html>'

        self.assertEqual(None, check_for_meta(content, ''))

    def test_prepare_url(self):
        url = 'http://test.com/ ~test;aasd ?a=&b=1'
        url_test = 'http://test.com/%20%7Etest;aasd+?a=&b=1'
        self.assertEqual(url_test, prepare_url(url))

    def test_prepare_url_none(self):
        url = None
        self.assertEqual(None, prepare_url(url))

    def test_get_url_meta(self):
        content = 'test'
        new_red = None
        new_red_meta = 'test'
        url_test = 'url_test'
        timeout_test = 400
        REDIRECT_META = 'meta_tag'

        make_pycurl_mock = mock.Mock(return_value=(content, new_red))
        check_for_meta_mock = mock.Mock(return_value=new_red_meta)
        with mock.patch('lib.__init__.make_pycurl_request', make_pycurl_mock):
            with mock.patch('lib.__init__.check_for_meta', check_for_meta_mock):
                with mock.patch('lib.__init__.prepare_url', mock.Mock(side_effect=prepare_url_fake)):
                    url, red, con = get_url(url_test, timeout_test)
                    make_pycurl_mock.assert_called_once_with(url_test, timeout_test, None)
                    self.assertEqual(new_red_meta, url)
                    self.assertEqual(REDIRECT_META, red)
                    self.assertEqual(content, con)

    def test_get_url_http(self):
        content = 'test'
        new_red = 'http://test.com'
        url_test = 'url_test'
        timeout_test = 400
        REDIRECT_HTTP = 'http_status'

        make_pycurl_mock = mock.Mock(return_value=(content, new_red))
        check_for_meta_mock = mock.Mock(return_value=new_red)
        with mock.patch('lib.__init__.make_pycurl_request', make_pycurl_mock):
            with mock.patch('lib.__init__.check_for_meta', check_for_meta_mock):
                with mock.patch('lib.__init__.prepare_url', mock.Mock(side_effect=prepare_url_fake)):
                    url, red, con = get_url(url_test, timeout_test)
                    make_pycurl_mock.assert_called_once_with(url_test, timeout_test, None)
                    self.assertEqual(new_red, url)
                    self.assertEqual(REDIRECT_HTTP, red)
                    self.assertEqual(content, con)

    def test_get_url_none(self):
        content = 'test'
        new_red = 'http://www.odnoklassniki.ru/st.redirect/'
        url_test = 'url_test'
        timeout_test = 400

        make_pycurl_mock = mock.Mock(return_value=(content, new_red))
        with mock.patch('lib.__init__.make_pycurl_request', make_pycurl_mock):
            with mock.patch('lib.__init__.prepare_url', mock.Mock(side_effect=prepare_url_fake)):
                url, red, con = get_url(url_test, timeout_test)
                make_pycurl_mock.assert_called_once_with(url_test, timeout_test, None)
                self.assertEqual(None, url)
                self.assertEqual(None, red)
                self.assertEqual(content, con)

    def test_get_url_market(self):
        content = 'test'
        new_red = 'market://www.test/'
        return_fix_market = 'http://play.google.com/store/apps/www.test/'
        url_test = 'url_test'
        timeout_test = 400
        REDIRECT_HTTP = 'http_status'

        make_pycurl_mock = mock.Mock(return_value=(content, new_red))
        fix_market_url_mock = mock.Mock(return_value=return_fix_market)
        with mock.patch('lib.__init__.make_pycurl_request', make_pycurl_mock):
            with mock.patch('lib.__init__.fix_market_url', fix_market_url_mock):
                with mock.patch('lib.__init__.prepare_url', mock.Mock(side_effect=prepare_url_fake)):
                    url, red, con = get_url(url_test, timeout_test)
                    make_pycurl_mock.assert_called_once_with(url_test, timeout_test, None)
                    fix_market_url_mock.assert_called_once_with(new_red)
                    self.assertEqual(return_fix_market, url)
                    self.assertEqual(REDIRECT_HTTP, red)
                    self.assertEqual(content, con)

    def test_get_url_error(self):
        url_test = 'url_test'
        timeout_test = 400

        make_pycurl_mock = mock.Mock()
        with mock.patch('lib.__init__.make_pycurl_request', make_pycurl_mock):
            make_pycurl_mock.side_effect = pycurl.error
            url, red, con = get_url(url_test, timeout_test)
            make_pycurl_mock.assert_called_once_with(url_test, timeout_test, None)
            self.assertEqual(url_test, url)
            self.assertEqual('ERROR', red)
            self.assertEqual(None, con)

    def test_get_redirect_history_empty(self):
        url_test = 'http://test.com'
        timeout_test = 777
        re_match_mock = mock.Mock(return_value=True)
        with mock.patch('lib.__init__.prepare_url', mock.Mock(side_effect=prepare_url)):
            with mock.patch('re.match', re_match_mock):
                history_type, history_url, counters = get_redirect_history(url_test, timeout_test)
                self.assertEqual([], history_type)
                self.assertEqual([url_test], history_url)
                self.assertEqual([], counters)

    def test_get_redirect_history_not_redirect_url(self):
        url_test = 'http://test.com'
        timeout_test = 777
        return_red_url = None
        return_content = None
        return_red_type = 'http_status'

        re_match_mock = mock.Mock(return_value=False)
        get_url_mock = mock.Mock(return_value=(return_red_url, return_red_type, return_content))
        with mock.patch('lib.__init__.prepare_url', mock.Mock(side_effect=prepare_url)):
            with mock.patch('re.match', re_match_mock):
                with mock.patch('lib.__init__.get_url', get_url_mock):
                    history_type, history_urls, counters = get_redirect_history(url_test, timeout_test)
                    get_url_mock.assert_called_once_with(url=url_test, timeout=timeout_test, user_agent=None)
                    self.assertEqual([], history_type)
                    self.assertEqual([url_test], history_urls)
                    self.assertEqual([], counters)

    def test_get_redirect_history_error(self):
        url_test = 'http://test.com'
        timeout_test = 777
        return_red_url = 'http://redirect/test.com'
        return_content = None
        return_red_type = 'ERROR'

        re_match_mock = mock.Mock(return_value=False)
        get_url_mock = mock.Mock(return_value=(return_red_url, return_red_type, return_content))
        with mock.patch('lib.__init__.prepare_url', mock.Mock(side_effect=prepare_url)):
            with mock.patch('re.match', re_match_mock):
                with mock.patch('lib.__init__.get_url', get_url_mock):
                    history_type, history_urls, counters = get_redirect_history(url_test, timeout_test)
                    get_url_mock.assert_called_once_with(url=url_test, timeout=timeout_test, user_agent=None)
                    self.assertEqual([return_red_type], history_type)
                    self.assertEqual([url_test, return_red_url], history_urls)
                    self.assertEqual([], counters)

    def test_get_redirect_history(self):
        url_test = 'http://test.com'
        timeout_test = 777
        max_redirects_test = 1
        return_red_url = 'http://redirect/test.com'
        return_content = None
        return_red_type = 'meta_tag'

        re_match_mock = mock.Mock(return_value=False)
        get_url_mock = mock.Mock(return_value=(return_red_url, return_red_type, return_content))
        with mock.patch('lib.__init__.prepare_url', mock.Mock(side_effect=prepare_url)):
            with mock.patch('re.match', re_match_mock):
                with mock.patch('lib.__init__.get_url', get_url_mock):
                    history_type, history_urls, counters = get_redirect_history(url_test, timeout_test, max_redirects_test)
                    get_url_mock.assert_called_with(url=url_test, timeout=timeout_test, user_agent=None)
                    self.assertEqual([return_red_type], history_type)
                    self.assertEqual([url_test, return_red_url], history_urls)
                    self.assertEqual([], counters)
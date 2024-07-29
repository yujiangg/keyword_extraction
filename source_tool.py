import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import DBhelper
import pandas as pd
import re



class SourceTool:
    
    
    def source_wraper(func):
        """
        """
        def wrapper(self, *args, **kargs):
            
            web_id, source = func(self, *args, **kargs)

            if not self.domain.get(web_id):
                return 'other'
            if re.findall(rf'[^\w]({source})[^\w]?', self.domain.get(web_id, {}).get('domain', '///'), flags=re.I):
                return web_id
            elif re.findall(rf'[^\w]({source})[^\w]?', self.domain.get(web_id, {}).get('subdomain', '///'), flags=re.I):
                return web_id
            elif re.findall(rf'[^\w]?({source})[^\w]?', 'appservice.91app.com', flags=re.I):
                return web_id
            elif source.lower() in self.source_list:
                return source.lower()
            elif source.lower() == 'gaii':
                return 'likr'
            elif re.findall('pixnet', source, flags=re.I):
                return web_id
            else:
                return 'other'
            
        return wrapper
            

    def __init__(self, web_ids: list=[]) -> None:
        
        if isinstance(web_ids, str):
            web_ids = [web_ids]
        self.web_ids = web_ids
        self.source_mapping = {
                        "facebook"                : "fb",
                        "fb"                      : "fb",
                        "DPA"                     : "fb",
                        "g(?:oogle)?ad"           : "gad",
                        "google"                  : "google search",
                        "(likr)(?:_remarketing)?" : "likr",
                        "ad?vivid"                : "likr",
                        "line"                    : "line",
                        "youtube"                 : "youtube",
                        r"yt[\W]"                 : "youtube",
                        "yahoo"                   : "yahoo",
                        r"bing[\W]"               : "microsoft",
                        "instagram(?:Shopping)?"  : "instagram",
                        "^ig(?:Shopping)?"        : "instagram",
                        "gaii.ai"                 : "gaii",
                        "shopback"                : "shopback",
                        "(ins(ider)?)(?!tagram)"  : "insider",
                        "Aideal"                  : "appier",
                        "app-carousel"            : "app-carousel",
                        "sms"                     : "sms",
                        "EDM"                     : "EDM",
                        "weixin"                  : "weixin"
                        }
        
        self.source_list = ['all', 'fb', 'google search', 'likr', 'line', 'youtube', 'yahoo', 'instagram', 'other', 'microsoft', 'sms', 'criteo',
                            'shopback', 'appier', 'ichannels', 'gad', 'bridgewell', 'insider'] #要新增
        
        self.subdomain = {'underwear':{'underwear':'emon'}, 'i3fresh':{'i3fresh':'mob.i3fresh'}, 'nineyi711':{'autocare.com':'autocare'}, 
                          'shopedh':{'shop.everydayhealth.com':'www.edh'}, 'nineyi62':{'boy2.91app':'www.boy2shop'}, 'cherif':{'-perfume.com':'tw'}, 
                          'nicelife':{'real':'-real'}, 'cowey':{'gewei':'cowey'}, 'chingtse':{'com':'com.tw'}, 
                          'nineyi':{'icarebeauty.91app.com':'www.icarebeauty.com.tw'}, 'ego':{'shop':'viva'}, 'parkcat':{'.com':''}, 
                          'buybiji':{'buy':'www.buy'}, 'kfan':{'www':'m'}, 'starkiki':{'kiki':'kikiam'}, 
                          'nineyi39805':{'lgcare.91app.com':'www.lglife.com.tw'}, 'nineyi1937':{'shop.opro9.com':'www.cvimall.net'}, 
                          'nineyi2012':{'niko':'www.niko'}, 'nanobiolight':{'.com':'cp.com'}, 'inparents':{'www':'babywearing'}, 
                          'nineyi000360':{'mart':'ap'}, 'playjoylube':{'www':'shop'}, 'addpureskin':{'add':'www.add'}, 
                          'avvjoy':{'com/zh-TW/':'cyberbiz.co'}, 'pure17':{'pure17go':'ftvnews'}
                          }
        self.domain = self._fetch_domain_url()
    
    @source_wraper
    def get_url_source(self, web_id: str, url_last: str, url_now: str, browser_type: str=''):
        '''
        check_url_source is utilized for checking the source of each browsing of product.

        Parameters
        ----------
        web_id : str
            The web_id that be checked.

        url_last : str
            The url of the previous page.
        
        url_now : str
            The url of the current page.
        
        browser_type : str
            The browser_type of the current session.
        
        Returns
        -------
        Str or Bool
            The name of the source or False if the url is invalid.
        '''

        url_last, url_now = str(url_last), str(url_now)

        # 1st step check source, if no source then check medium
        source = self._get_utm(url_now)
        if source:
            return web_id, source
        
        # 2nd step
        if browser_type:
            return web_id, browser_type
        
        # 3rd step
        if any(url==bad_pattern for bad_pattern in ['None', '', '_', '-1'] for url in [url_last, url_now]):
            return web_id, "other"
        
        if url_now.count('/') < 2 or url_last.count('/') < 2:
            return web_id, "other"
        
        domain_now = url_now.split('/')[2]
        domain_last = url_last.split('/')[2]

        if domain_now != domain_last:

            for k, v in self.source_mapping.items():
                pattern = rf'(?:[\W]|^)({k})'
                if re.findall(pattern, domain_last, flags=re.I):
                    return web_id, v
        
        # 4th step
        source = self._get_utm(url_last)
        if source:
            return web_id, source

        return web_id, domain_last


    def _fetch_domain_url(self):
        '''
        '''

        web_id = "','".join(self.web_ids)
        query = f'''SELECT web_id, `domain` FROM web_push.all_website_category
                WHERE web_id in ('{web_id}') GROUP BY web_id;'''
        data = DBhelper('db_subscribe', is_ssh=True).ExecuteSelect(query)
        if len(data) != len(self.web_ids):
            web_ids = [i for i in self.web_ids if i not in [row['web_id'] for row in data]]
            raise ValueError(f'{web_ids} is/are not correct web_id(s)')
        domains = pd.DataFrame(data, columns=['web_id', 'domain'])
        domains['subdomain'] = domains.apply(lambda x: x['domain'].replace(*list(self.subdomain.get(x['web_id'], {x['web_id']:x['web_id']}).items())[0]), axis=1)
        domains = domains.set_index('web_id').to_dict('index')

        return domains
    
        
    def _get_utm(self, url):
        """
        """

        source = re.findall(r'(?<=utm_source=)[^&]+', str(url))
        if source:
            source = source[-1]
            for k, v in self.source_mapping.items():
                pattern = rf'{k}'
                if re.findall(pattern, source, flags=re.I):
                    return v
            return source
        
        medium = re.findall(r'(?<=utm_medium=)[^&]+', str(url))
        if medium:
            medium = medium[-1]
            for k, v in self.source_mapping.items():
                pattern = rf'{k}'
                if re.findall(pattern, medium, flags=re.I):
                    return v
            return medium
        
        return False

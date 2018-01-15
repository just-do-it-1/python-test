# coding=utf-8
import codecs, sys, re, json, os, time, hashlib, datetime, chardet, sched, threading, subprocess
reload(sys)
sys.setdefaultencoding('utf-8')
# sys.path.append('../')
sys.path.append('/home/bastion/spider_zol/')
sys.setrecursionlimit(10000)
from libnary.web_spider_datas import WebSpider
from libnary.analysis_web_datas import AnalysisWebDatas
from utils.mogojd import yJdItem
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)
# chcp 65001 
# set PYTHONIOENCODING=UTF-8

class jd_job:

    def __init__(self):
        self.writeLog("开始抓取京东电商")
        jdUrls = self.analysisUrlDatas()
        self.createJob(jdUrls)

    def orderSort(self):
        return ['sort_totalsales15_desc', 'sort_dredisprice_desc', 'sort_commentcount_desc', 'sort_winsdate_desc']

    def getUrlTemplate(self):
        return {
            'catUrl': 'https://list.jd.com/list.html?cat={{catId}}&page={{pageNumbers}}&sort=sort_commentcount_desc&trans=1&JL=6_0_0&ms=5#J_main',
            'tidUrl': 'https://list.jd.com/list.html?tid={{tId}}&page={{pageNumbers}}&sort=sort_commentcount_desc&trans=1&JL=6_0_0&ms=5#J_main'
        }

    def getJsonOverSeasTemplates(self):
        return 'https://club.jd.com/productpage/p-{{productId}}-s-0-t-1-p-{{pageNumbers}}.html?_=1496371382142'

    def getJsonTemplate(self):
        return 'https://club.jd.com/comment/productPageComments.action?productId={{productId}}&score=0&sortType=6&page={{pageNumbers}}&pageSize={{sizeNumbers}}&isShadowSku=0&isShadowSku=0&fold=1'

    def getUrlModels(self):
        return [
            {'main':'电脑、办公','title':'电脑整机','subtitle':'台式机','tid':'0','cat':'670,671,673', 'level': 1},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'游戏电脑','tid':'1000006','cat':'0', 'level': 2},
            {'main':'电脑、办公','title':'电脑整机','subtitle':'一体机','tid':'0','cat':'670,671,12798', 'level': 2},
            {'main':'电脑、办公','title':'电脑整机','subtitle':'平板电脑','tid':'0','cat':'670,671,2694', 'level': 2},
            {'main':'电脑、办公','title':'电脑整机','subtitle':'笔记本','tid':'0','cat':'670,671,672', 'level': 2},
            {'main':'电脑、办公','title':'外设产品','subtitle':'键盘','tid':'0','cat':'670,686,689', 'level': 9},
            {'main':'电脑、办公','title':'外设产品','subtitle':'鼠标','tid':'0','cat':'670,686,690', 'level': 9},
            {'main':'手机','title':'手机通讯','subtitle':'手机','tid':'0','cat':'9987,653,655', 'level': 2},
            {'main':'电脑、办公','title':'电脑整机','subtitle':'游戏本','tid':'0','cat':'670,671,1105', 'level': 2},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'办公电脑','tid':'1000007','cat':'0', 'level': 2},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'内存','tid':'0','cat':'670,677,680', 'level': 3},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'机箱','tid':'0','cat':'670,677,687', 'level': 6},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'电源','tid':'0','cat':'670,677,691', 'level': 3},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'显示器','tid':'0','cat':'670,677,688', 'level': 3},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'刻录机/光驱','tid':'0','cat':'670,677,684', 'level': 3},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'散热器','tid':'0','cat':'670,677,682', 'level': 4},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'声卡/扩展卡','tid':'0','cat':'670,677,5008', 'level': 4},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'组装电脑','tid':'0','cat':'670,677,11762', 'level': 8},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'装机配件','tid':'0','cat':'670,677,5009', 'level': 4},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'高清显示器','tid':'0','cat':'670,677,1000131', 'level': 8},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'游戏显示器','tid':'0','cat':'670,677,1000132', 'level': 8},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'大屏显示器','tid':'0','cat':'670,677,1000133', 'level': 8},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'曲面屏显示器','tid':'0','cat':'670,677,1000134', 'level': 8},
            {'main':'手机','title':'手机配件','subtitle':'配件详情','tid':'0','cat':'9987,830', 'level': 10},
            {'main':'电脑、办公','title':'游戏设备','subtitle':'游戏设备详情','tid':'0','cat':'670,12800', 'level': 5},
            {'main':'手机','title':'手机通讯','subtitle':'老人机','tid':'1001056','cat':'0', 'level': 5},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'MOD改装','tid':'1000008','cat':'0', 'level': 5},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'带鱼屏显示器','tid':'0','cat':'670,677,1000135', 'level': 7},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'CPU','tid':'0','cat':'670,677,678', 'level': 6},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'主板','tid':'0','cat':'670,677,681', 'level': 6},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'硬盘','tid':'0','cat':'670,677,683', 'level': 6},
            {'main':'电脑、办公','title':'电脑配件','subtitle':'SSD固态硬盘','tid':'0','cat':'670,677,11303', 'level': 3},
            {'main':'电脑、办公','title':'电脑整机','subtitle':'平板电脑配件','tid':'0','cat':'670,671,5146', 'level': 10},
            {'main':'电脑、办公','title':'电脑整机','subtitle':'笔记本','tid':'0','cat':'670,671,672&ev=exbrand_11516', 'level': 11},
            {'main':'电脑、办公','title':'电脑整机','subtitle':'台式机','tid':'0','cat':'670,671,673&ev=exbrand_11516', 'level': 11},
            {'main':'手机','title':'手机通讯','subtitle':'手机','tid':'0','cat':'9987,653,655&ev=exbrand_11516', 'level': 11},
            {'main':'手机','title':'手机通讯','subtitle':'手机','tid':'0','cat':'9987,653,655&ev=exbrand_13066', 'level': 11},
            {'main':'手机','title':'手机通讯','subtitle':'手机','tid':'0','cat':'9987,653,655&ev=exbrand_134922', 'level': 11},
            {'main':'电脑、办公','title':'电脑整机','subtitle':'服务器/工作站','tid':'0','cat':'670,671,674', 'level': 11},
            {'main':'电脑、办公','title':'电脑整机','subtitle':'笔记本配件','tid':'0','cat':'670,671,675', 'level': 9}
        ]

    def analysisUrlDatas(self, pageNumbers='1'):
        urlsDatas = self.getUrlModels()
        urlTemplates = self.getUrlTemplate()
        pageDatas = re.compile('{{pageNumbers}}')
        catId = re.compile('{{catId}}')
        tId = re.compile('{{tId}}')
        resultPage = ''
        orderPage = ''
        listDatas = []
        for data in urlsDatas:
            if data['tid'] == '0':
                orderPage = catId.sub(str(data['cat']), urlTemplates['catUrl'])
            else:
                orderPage = tId.sub(str(data['tid']), urlTemplates['tidUrl'])
            listDatas.append({'pageDatas': data, 'pageUrl': pageDatas.sub(pageNumbers, orderPage), 'urlTemplate': orderPage, 'level': data['level']})
        return listDatas

    def writeLog(self, datas):
        print "date:=>%s, datas:=>%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), datas)

    def getJDMaxPages(self, datas, analysis):
        pages = ''
        orderPages = []
        if len(datas):
            pages = str(datas[0]).encode('utf-8', 'ignore')
            for page in analysis.getByTagNameText(pages, 'a'):
                pageNumbers = analysis.strToInt(page)
                if len(pageNumbers):
                    orderPages.append(int(pageNumbers[0]))
            return max(orderPages) + 1
        return 2

    def getUrlMaxPages(self, urlDatas, spider, analysis):
        pageDatas = re.compile('{{pageNumbers}}')
        self.writeLog(urlDatas['pageUrl'])
        datas = spider.getWebDatas(urlDatas['pageUrl'])
        maxPage = self.getJDMaxPages(analysis.getById(datas, '#J_bottomPage'), analysis)
        for page in range(1, maxPage):
            urls = pageDatas.sub(str(page), urlDatas['urlTemplate'])
            self.writeLog("the web spider url is : " + urls)
            cateDatas = urlDatas['pageDatas']
            webDatas = spider.getWebDatas(urls)
            listPages = analysis.getById(webDatas, '#plist')
            self.getGoodsId(analysis.getByClassName(listPages, 'div', 'p-img'), cateDatas, spider, analysis)
    
    def getGoodsId(self, goodsImages, cateDatas, spider, analysis):
        urlTemplates = self.getJsonTemplate()
        pid = re.compile('{{productId}}')
        sizeNumbers = re.compile('{{sizeNumbers}}')
        pageNumbers = re.compile('{{pageNumbers}}')
        page = 0
        pageSize = 10
        goodsUrls = ''
        resultUrls = ''
        for data in goodsImages:
            datasUrls = 'https:' + analysis.getWebUrl(analysis.getByTagName(data, 'a'))
            self.writeLog("the web detail spider url is : " + datasUrls)
            counterDatas = yJdItem.objects.count()
            print counterDatas
            goodsId = analysis.strToInt(datasUrls)
            goodsUrls = pid.sub(str(goodsId[0]), urlTemplates)
            goodsUrls = sizeNumbers.sub(str(pageSize), goodsUrls)
            resultUrls = pageNumbers.sub(str(page), goodsUrls)
            jsonDatas = spider.getWebDatasJsonOrders(resultUrls)
            self.writeLog(resultUrls)
            if self.isPageDatas(jsonDatas):
                maxPage = int(jsonDatas['maxPage'])
                self.getGoodsDatas(maxPage, str(goodsId[0]), goodsUrls, datasUrls, jsonDatas, cateDatas, spider, analysis)
    
    def isPageDatas(self, jsonDatas):
        if isinstance(jsonDatas, str):
            return False
        else :
            if jsonDatas.has_key('maxPage'):
                return True
            else:
                return False 

    def getDiscuz(self, datas):
        socre = int(datas)
        if socre == 5:
            return '好评'
        elif socre == 4:
            return '中评'
        elif socre == 3:
            return '中评'
        elif socre == 2:
            return '差评'
        elif socre == 1:
            return '差评'
        else:
            return '差评'

    def get_tid(self, datas):
        m2 = hashlib.md5()   
        m2.update(datas)   
        return m2.hexdigest() 

    def getGoodsDatas(self, maxPage, goodsId, goodsUrls, datasUrls, jsonDatas, cateDatas, spider, analysis):
        pageNumbers = re.compile('{{pageNumbers}}')
        orderDatas = []
        commentInfo = []
        itemId = analysis.strToInt(datasUrls)
        categories = "京东电商-" + str(cateDatas['main']) + '-' + str(cateDatas['title']) + '-' + str(cateDatas['subtitle'])
        hotTags = self.getHotTages(jsonDatas)
        goodsTitle = self.getGoodsTitles(datasUrls, spider, analysis)
        for page in range(0, maxPage):
            resultUrls = pageNumbers.sub(str(page), goodsUrls)
            jsonDatas = spider.getWebDatasJsonOrders(resultUrls)
            if self.datasProcess(jsonDatas):
                self.getGoodsDataInfo(jsonDatas, datasUrls, resultUrls, goodsTitle, categories, hotTags, goodsId, itemId, spider, analysis)

    def getCollectComment(self, datas):
        comments = []
        for data in datas:
            for dd in data['goodsCommentInfo']:
                comments.append(dd)
        return comments

    def getCollectGoodsInfo(self, datas):
        goodsInfo = []
        for data in datas:
            for dd in data['goodsInfo']:
                goodsInfo.append(dd)
        return goodsInfo

    def datasProcess(self, jsonDatas):
        if isinstance(jsonDatas, str):
            return False
        else:
            if jsonDatas.has_key('comments'):
                if isinstance(jsonDatas['comments'], list):        
                    if len(jsonDatas['comments']) >= 1:
                        return True
                    else:
                        return False
                else:
                    return False
        return False
        
    def writeFile(self, fileName, path, content, fileCharset):
        file = codecs.open(path+fileName, 'wb', fileCharset)
        file.write(content)
        file.close()
    
    def getGoodsDetails(self, jsonData):
        goods = jsonData['referenceName']
        if jsonData.has_key('productColor'):
            goods += jsonData['productColor']
        if jsonData.has_key('productSize'):
            goods += jsonData['productSize']
        return goods

    def getProductRepliesDatas(self, jsonData, spider, analysis):
        commentDatas = []
        dictDatas = {}
        referenceIds = re.compile('{{referenceId}}')
        guids = re.compile('{{guid}}')
        pageNumbers = re.compile('{{pageNumbers}}')
        commentUrls = 'https://club.jd.com/repay/{{referenceId}}_{{guid}}_{{pageNumbers}}.html'
        if jsonData.has_key('referenceId') and jsonData.has_key('guid'):
            resultPage = referenceIds.sub(str(jsonData['referenceId']), commentUrls)
            resultPage = guids.sub(str(jsonData['guid']), resultPage)
            resultPage = pageNumbers.sub('1', resultPage)
            self.writeLog("the web page is : " + resultPage)
            datas = spider.getWebDatas(resultPage)
            if len(analysis.getByClassName(datas, 'div', 'ui-page')):
                pages = self.getJDMaxPages(analysis.getByClassName(datas, 'div', 'ui-page'), analysis)
                if pages == 2:
                    comments = analysis.getByTagNameCommon(datas, 'div.item')
                    for comment in comments:
                        if len(analysis.getByTagNameCommon(comment, 'div.tt')) >= 1 and len(analysis.getByTagNameCommon(comment, 'span.time')) >= 1:
                            dictDatas = {
                                'replies_content': analysis.getElementsText(analysis.getByTagNameCommon(comment, 'div.tt')[0]),
                                'replies_time': analysis.getElementsText(analysis.getByTagNameCommon(comment, 'span.time')[0])
                            }
                            commentDatas.append(dictDatas)
                    return commentDatas
                elif pages > 2:
                    for page in range(1, pages):
                        resultPage = referenceIds.sub(str(jsonData['referenceId']), commentUrls)
                        resultPage = guids.sub(str(jsonData['guid']), resultPage)
                        resultPage = pageNumbers.sub(str(page), resultPage)
                        self.writeLog("the web page is : " + resultPage)
                        datas = spider.getWebDatas(resultPage)
                        comments = analysis.getByTagNameCommon(datas, 'div.item')
                        for comment in comments:
                            if len(analysis.getByTagNameCommon(comment, 'div.tt')) >= 1 and len(analysis.getByTagNameCommon(comment, 'span.time')) >= 1:
                                dictDatas = {
                                    'replies_content': analysis.getElementsText(analysis.getByTagNameCommon(comment, 'div.tt')[0]),
                                    'replies_time': analysis.getElementsText(analysis.getByTagNameCommon(comment, 'span.time')[0])
                                }
                                commentDatas.append(dictDatas)
                    return commentDatas
                else:
                    return commentDatas
            else:
                return commentDatas
        else:
            return commentDatas

    def getGoodsDataInfo(self, jsonDatas, datasUrls, commentUrl, title, categories, hotTags, goodsId, itemId, spider, analysis):
        referenceIds = re.compile('{{referenceId}}')
        guids = re.compile('{{guid}}')
        pageNumbers = re.compile('{{pageNumbers}}')
        idIndex = ''
        for jsonData in jsonDatas['comments']:
            idIndex = self.get_tid(jsonData['guid']+jsonData['referenceId'])
            # jdDatas = yJdItem.objects.filter(_id=idIndex)
            # if len(jdDatas) > 0:
            #      continue
            replies_comment_datas = self.getProductRepliesDatas(jsonData, spider, analysis)
            commentUrls = 'https://club.jd.com/repay/{{referenceId}}_{{guid}}_{{pageNumbers}}.html'
            resultPage = referenceIds.sub(str(jsonData['referenceId']), commentUrls)
            resultPage = guids.sub(str(jsonData['guid']), resultPage)
            resultPage = pageNumbers.sub('1', resultPage)
            if len(title) <= 0:
                title = jsonData['goodsTitle']
            last_reply_time = self.getLastReplyTime(replies_comment_datas, jsonData['creationTime'])
            jd_datas = yJdItem(
                _id=idIndex,
                itemId=str(itemId[0]),
                title=title,
                url=datasUrls,
                content_url=resultPage,
                source='京东电商',
                source_short='jd',
                insert_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                category=categories,
                hotTags=hotTags,
                flag='-1',
                replies=str(jsonData['replyCount']),
                views=str(jsonData['viewCount']),
                goods_score_name=self.getDiscuz(jsonData['score']),
                score=str(jsonData['score']),
                product_detail=self.getGoodsDetails(jsonData),
                content=jsonData['content'],
                time=jsonData['creationTime'],
                order_time=jsonData['referenceTime'],
                replies_comment=replies_comment_datas,
                comment_guid=jsonData['guid'],
                comment_id=jsonData['referenceId'],
                user_level_name=str(jsonData['userLevelName']),
                useless_vote_count=str(jsonData['uselessVoteCount']),
                useful_vote_count=str(jsonData['usefulVoteCount']),
                nick_name=str(jsonData['nickname']),
                user_client=str(jsonData['userClientShow']),
                last_reply_time=last_reply_time,
                v='0.1'
            )
            jd_datas.save()

    def getLastReplyTime(self, datas, time):
        if len(datas) >= 1:
            for data in datas:
                return data['replies_time']
        else :
            return time

    def getGoodsTitles(self, productUrl, spider, analysis):
        datas = spider.getWebRedirctionDatas(productUrl)
        for data in analysis.getByClassName(datas, 'div', 'sku-name'):
            return analysis.getElementsText(data)

    def getHotTages(self, jsonDatas):
        hotTags = []
        if jsonDatas.has_key('hotCommentTagStatistics'):
            for data in jsonDatas['hotCommentTagStatistics']:
                hotTags.append(data['name'])
        return hotTags

    def createJob(self, urlDatas):
        spider = WebSpider()
        analysis = AnalysisWebDatas()
        scheduler = sched.scheduler(time.time, time.sleep)
        order_timer = 2
        for datas in urlDatas:
            scheduler.enter(order_timer, datas['level'], self.getUrlMaxPages, (datas, spider, analysis))
            order_timer += 2
        thread = threading.Thread(target=scheduler.run) 
        thread.start()
        # action2 = scheduler.enter(3, 1, increment_counter, ('action2',)) 
        # scheduler.cancel(action1) 
        # t.join()

jd_job()

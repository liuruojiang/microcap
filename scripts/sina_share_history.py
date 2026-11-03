"""Strict parser candidate for preserved native Sina TotalStock pages.

No HTTP, production adoption or certified cutoff creation occurs here. Dates
remain provider-labelled change/report dates, not inferred legal effect dates.
"""
from datetime import date
from decimal import Decimal,InvalidOperation
import hashlib
import re
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup


def _require(condition,message):
    if not condition:raise ValueError('Sina TotalStock: '+message)


def _day(value):
    _require(isinstance(value,str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}',value),'invalid event date')
    return date.fromisoformat(value)


def _amount(value):
    _require(isinstance(value,str) and re.fullmatch(r'\d+(?:\.\d{1,4})?',value) is not None,'invalid exact 万股 amount')
    try:number=Decimal(value)
    except InvalidOperation as error:raise ValueError('Sina TotalStock: invalid decimal') from error
    _require(number.is_finite() and number>0 and number*10000==(number*10000).to_integral_value(),'not positive whole shares')
    return number


def parse_sina_total_history(raw,*,symbol,observed_session,allowed_future_plans=None):
    _require(isinstance(raw,bytes) and raw,'empty/non-byte response')
    _require(isinstance(symbol,str) and re.fullmatch(r'[0-9]{6}',symbol),'invalid symbol')
    target=_day(observed_session)
    # The caller must derive these exact quantities/dates only after its
    # independent EM + issuer-PDF review. A manifest assertion is not approval.
    plans={}
    if allowed_future_plans is not None:
        _require(isinstance(allowed_future_plans,list),'future-plan allowlist must be a list')
        for plan in allowed_future_plans:
            _require(isinstance(plan,dict) and set(plan)=={'date','total_shares_10k'},'unknown/missing future-plan fields')
            day=plan['date'];amount=_amount(plan['total_shares_10k'])
            _require(_day(day)>target and day not in plans,'nonfuture/duplicate reviewed plan')
            plans[day]=amount
    def check_date(day,amount):
        _require(_day(day)<=target or plans.get(day)==amount,'future event/report date without exact reviewed plan')
    text=raw.decode('gb18030',errors='strict')
    _require('</html>' in text.lower(),'truncated HTML document')
    soup=BeautifulSoup(text,'html.parser')
    _require(len(soup.find_all('title'))==1,'missing/duplicate title')
    title=soup.title.get_text(' ',strip=True)
    identity=re.fullmatch(r'(.+)\('+re.escape(symbol)+r'\)股本变动历史_新浪财经_新浪网',title)
    _require(identity is not None,'title issuer/symbol mismatch')
    issuer=identity.group(1)
    containers=soup.find_all(id='StockStructureHistoryTable')
    _require(len(containers)==1,'missing/duplicate complete history container')
    container=containers[0]
    _require(container.find(class_='grayTitlebg1').get_text(strip=True)=='总股本的变动历史','not total-stock history')
    _require(not container.find_all('a'),'unexpected paging/navigation inside complete history')
    params=[n for n in container.find_all('param') if n.get('name')=='FlashVars']
    embeds=container.find_all('embed')
    _require(len(params)==1 and len(embeds)==1,'missing/duplicate native chart layer')
    graph_inventories=[]
    caption=None
    for payload in (params[0].get('value',''),embeds[0].get('flashvars','')):
        _require(payload.startswith('&dataXML=<graph ') and payload.endswith('</graph>'),'incomplete native graph payload')
        graph=ET.fromstring(payload[len('&dataXML='):])
        _require(graph.tag=='graph','unexpected graph root')
        got_caption=' '.join(graph.attrib.get('caption','').split())
        _require(got_caption==issuer+'('+symbol+') 总股本变动历史','graph issuer/stocktype mismatch')
        caption=got_caption
        rows=[]
        for node in graph:
            _require(node.tag=='set' and set(node.attrib)=={'name','value','hoverText'},'unknown/missing chart point fields')
            day=node.attrib['hoverText']
            number=_amount(node.attrib['value'])
            check_date(day,number)
            rows.append((day,number))
        _require(rows and [r[0] for r in rows]==sorted(set(r[0] for r in rows)),'empty/duplicate/unsorted chart inventory')
        graph_inventories.append(rows)
    _require(graph_inventories[0]==graph_inventories[1],'PARAM/EMBED inventories differ')
    all_tables=container.find_all('table')
    wrappers=[t for t in all_tables if t.find('table') is not None]
    _require(len(wrappers)==1 and wrappers[0].get('id') is None,'unexpected complete-history layout wrapper')
    tables=[t for t in all_tables if t.find('table') is None]
    _require(tables and len({t.get('id') for t in tables})==len(tables),'missing/duplicate native year table')
    table_rows=[];table_inventory=[]
    for table in tables:
        name=table.get('id','')
        _require(re.fullmatch(r'historyTable[0-9]+',name) is not None,'unknown nested history table')
        current_year=None;count=0;years=[]
        for row in table.find_all('tr'):
            cells=[c.get_text(' ',strip=True) for c in row.find_all(['th','td'],recursive=False)]
            _require(len(cells)==2,'unknown/missing history row fields')
            header=re.fullmatch(r'(\d{4})年变动日期',cells[0])
            if header:
                _require(cells[1]=='持有股数','unexpected native header/unit semantics')
                current_year=int(header.group(1));years.append(current_year);continue
            day=cells[0]
            _require(current_year is not None and _day(day).year==current_year,'year/date mismatch')
            amount=re.fullmatch(r'(\d+(?:\.\d{1,4})?)万股',cells[1])
            _require(amount is not None,'table does not declare exact 万股')
            number=_amount(amount.group(1));check_date(day,number)
            table_rows.append((day,number));count+=1
        table_inventory.append(dict(table_id=name,year_headers=years,data_rows=count))
    _require(table_rows==graph_inventories[0],'visible year tables do not reproduce both complete native charts')
    actual_future={day:value for day,value in table_rows if _day(day)>target}
    _require(actual_future==plans,'unused reviewed future plan or mismatched future inventory')
    rows=[dict(provider_date=day,total_shares_10k=str(value),total_shares=str(int(value*10000))) for day,value in table_rows]
    return dict(symbol=symbol,issuer=issuer,title=title,graph_caption=caption,
        stocktype='TotalStock',unit='万股',raw_sha256=hashlib.sha256(raw).hexdigest(),
        native_graph_layers=2,graph_rows=len(rows),table_inventory=table_inventory,records=rows,
        declared_data_as_of=None,declared_complete_through_session=None,
        observed_session_context=observed_session,
        date_semantics='provider_change_or_report_date_not_assumed_effective_date',
        caller_reviewed_future_plans=[dict(date=day,total_shares_10k=str(value)) for day,value in plans.items()],
        production_adoption=False)

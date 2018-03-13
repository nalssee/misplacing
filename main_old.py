"""
"""
#%%
from sqlplus import *
import statistics as st
from itertools import product, chain
from scipy.stats import ttest_1samp

#
# buy and hold return
def bhr(rs):
    result = 1
    for r in rs:
        result *= 1 + r.ret / 100
    return result - 1



def append_yyyymm(r):
    r.yyyymm = strptime(r.date, '%Y-%m-%d').strftime('%Y%m')
    return r


def zerochunks(col, n):
    def fn(rs):
        rs0 = rs.where(lambda r: r[col] == 0)
        rs1 = rs.where(lambda r: r[col] > 0)
        yield rs0
        yield from rs1.chunks(n - 1)
    return fn


def stars(pval):
    if pval <= 0.01:
        return "***"
    elif pval <= 0.05:
        return "**"
    elif pval <= 0.10:
        return "*"
    return ""


def diff(high, low):
    seq = []
    for a, b in zip(high, low):
        seq.append(a - b)
    return seq


def ttest(seq, n=3):
    tval, pval = ttest_1samp(seq, 0.0)
    return f'{round(st.mean(seq), n)}{stars(pval)}', round(tval, n)



# setwd('/home/kenjin/work/mispricing/ws')
setwd("C:\\Users\\kenjin\\work\\mispricing\\ws")

print('ready..')


# * mdata.csv
# * 받은 날짜: 2016년 10월 28일
# * 기간: 1980년부터 받은 날짜까지, 월간
#
# * Non Trading day: Null, Include Weekend: All
# * 수익률(현금배당반영) (%), 시가총액(상장예정주식 포함) (백만원), 거래대금누계 (원), 총자본(천원), 우선주자본금(천원)
#%%
with dbopen('db') as c:
    c.load('mdata.csv', fn=append_yyyymm)



# mprc
#%%
with dbopen('db') as c:
    c.load('mprc.csv', fn=append_yyyymm)


# * raw/manal.csv
# * 받은 날짜: 2016년 10월 28일
# * 기간: 1980년부터 받은 날짜까지, 월간
# * Non Trading day: Null, Include Weekend: All
# * 투자의견전체수(E3)
#%%
with dbopen('db') as c:
    c.load('manal.csv', fn=append_yyyymm)



# 인로씨 ff5 files
#%%
with dbopen('db') as c:
    c.load('ff5_ew_mine.sas7bdat', 'ff5ew', fn=append_yyyymm)
    c.load('ff5_vw_mine.sas7bdat', 'ff5vw', fn=append_yyyymm)


# * raw/indcode.csv
# * Last Refresh: 2016-10-27 10:59:27
# * Calendar Basis
# * Portfolio	 	Default
# * Item
# * frequency	월간	KRW
# * Non-Trading Day	NULL	Asc
# * Include Weekend	ALL
# * Term	19800101	Current(20161026)
# * Symbol	A005930	A005930	A005930
# * Symbol Name	삼성전자	삼성전자	삼성전자
# * Kind	COM	COM	COM
# * Item	CP10005400	CP10001700	CP10001394
# * Item Name	거래소(시장)	기업명 (한글)	한국표준산업분류코드9차(대분류)
#%%
with dbopen('db') as c:
    def mktfn(r):
        r.yyyymm = dconv(r.date, '%Y-%m-%d', '%Y%m')
        if r.mkt == '유가증권시장':
            r.mkt = 'kospi'
        elif r.mkt == '코스닥':
            r.mkt = 'kosdaq'
        return r

    c.load('indcode.csv', fn=mktfn)


# 모멘텀 변수 만들기

# 일단 join 을 하나 해야되는데 이게 좀 오래걸려 그래서 따로 뺌
#%%
with dbopen('db') as c:
    # 보통 직전월 size 는 필요하니까 넣자
    c.create("""
    select *, dmath(yyyymm, '1 month', '%Y%m') as yyyymm1 from mdata
    """, 'mdata_tmp')
    # mdata 에 indcode, prc 붙이고
    c.join(
        ['mdata', '*', 'yyyymm, id'],
        ['mdata_tmp', 'size as size1', 'yyyymm1, id'],
        ['indcode', 'mkt, icode'],
        ['manal', 'anal'],
        ['mprc', 'prc'],
        name='mdata1',
        pkeys='yyyymm, id'
    )
    c.drop('mdata_tmp')

#%%
# mometum computation
# 상장이후 폐지 각각 6개월 제거, 가격 1000원 미만 제거
with dbopen('db') as c:
    mom_periods = [3, 6, 9, 12]

    c.drop('mom')
    for rs in c.fetch('mdata1', group='id', where="""
    isnum(ret, size, tvol, prc) and
    prc > 0 and size > 0 and tvol >= 0
    """):
        rs.order('yyyymm')
        begdate, enddate = rs[0].date, rs[-1].date
        for p in mom_periods:
            # start and end 6 months cut
            # data span is 1980 and 2016.9
            # and since I am interested only on 1999 to 2015 the following
            # is about fine
            rs1 = rs[6:-6]
            for rs2 in rs1.roll(p, 1, 'yyyymm'):
                if rs2.isconsec('yyyymm', '1 month', '%Y%m'):
                    # most recent one
                    r0 = rs2[-1]
                    # No penny stock for the whole rs2 period
                    if len(rs2.where(lambda r: r.prc < 1000)) == 0:
                        r = Row()
                        r.yyyymm = r0.yyyymm
                        r.id = r0.id
                        r.mkt = r0.mkt
                        r.icode = r0.icode
                        r.begdate = begdate
                        r.enddate = enddate
                        r.mom = p
                        r.momret = bhr(rs2)
                        r.prc = r0.prc
                        r.size = r0.size
                        r.size1 = r0.size1
                        r.tvol = r0.tvol
                        r.ret = r0.ret
                        r.anal = r0.anal
                        c.insert(r, 'mom', pkeys="yyyymm, id, mom")


#%%
# 1 way 로 패턴 한번 보까?
# 우선 risk free return 이 있어야 excess return 이 필요

# We need risk free return, but I have only daily rf
# So I need to convert it to realized monthly rf
#%%
with dbopen('db') as c:
    c.load('drfree.csv', fn=append_yyyymm)

# one shot fn
def comp_rf(r0, r1):
    a = 1 / (1 + r0)
    b = 1 / (1 + r1) ** (11 / 12)
    return (b - a) / a

#%%
with dbopen('db') as c:
    c.drop('rf_temp')
    for rs in c.fetch('drfree', group='yyyymm', where="isnum(rf)"):
        c.insert(rs.order('date')[-1], 'rf_temp')

    c.drop('rf')
    for rs in c.fetch('rf_temp', roll=(2, 1, 'yyyymm'), where="""
    isnum(rf) """):
        # No need to check if it's consecutive
        r0, r1 = rs
        r = Row()
        r.yyyymm = r1.yyyymm
        r.rf = comp_rf(r0.rf / 100, r1.rf / 100)
        c.insert(r, 'rf')
    c.drop('rf_temp')



# 우선 넘버링부터
#%%
# 'momavg1'
with dbopen('db') as c:
    mom_periods = [3, 6, 9, 12]
    # TODO: Holding 을 더 늘려서 reversal check 어떻게 할지 알아봐
    holding_periods = [3, 6, 9, 12]
    # holding_periods = [1]
    # 포트폴리오 크기
    psize = 4
    tname = 'momavg' + str(psize)
    c.drop('temptable')
    for mp, hp in product(mom_periods, holding_periods):
        # Kosdaq 을 포함할수도 아닐수도
        for rs in c.fetch('mom', roll=(hp + 2, 1, 'yyyymm', True), where=f"""
                           mom={mp} and (mkt='kospi' or mkt='kosdaq')
                           and yyyymm >= 200012 and yyyymm <= 201512
                           """):
            fdate = rs[0].yyyymm
            hdate = dmath(fdate, '2 months', '%Y%m')
            rs0 = rs.where(f'yyyymm = {fdate}')
            # 여기서 rs1 이 비어있을수도 있어
            rs1 = rs.where(f'yyyymm >= {hdate}')

            rs0.numbering({'momret': psize})
            rs1.follow(rs0, 'id', 'pn_momret')

            # 여기서 넘버링 했잖니 그럼 바로 평균내서 넣어야해.
            # 그럼 overlap 되는 애들 생기거든. rollover 했으니까
            for rs2 in rs1.isnum('pn_momret').group('yyyymm'):
                for rs3 in rs2.group('pn_momret'):
                    r = Row()
                    r.yyyymm = rs2[0].yyyymm
                    r.j = mp
                    r.k = hp
                    r.n = len(rs3)
                    r.pn = rs3[0].pn_momret
                    r.avgret = rs3.avg('ret')
                    # avg 는 지가 filtering 그냥 한다. 주의!!
                    r.wavgret = rs3.avg('ret', 'size1')
                    c.insert(r, 'temptable')

                # 해당월의 전체평균도 구해봐야지, 순서 같아야해 주의!
                r = Row()
                r.yyyymm = rs2[0].yyyymm
                r.j = mp
                r.k = hp
                r.n = len(rs2)
                # 전체 평균 0 으로 하는건 내 convention
                r.pn = 0
                # avg 는 지가 filtering 그냥 한다. 주의!!
                r.avgret = rs2.avg('ret')
                r.wavgret = rs2.avg('ret', 'size1')
                c.insert(r, 'temptable')

    # 위처럼 하면 겹치는 애들이 생기는데 걔네들을 평균해야대
    c.drop(tname)
    for rs in c.fetch('temptable', group='yyyymm, j, k, pn'):
        r = Row()
        r0 = rs[0]
        r.yyyymm = r0.yyyymm
        r.j = r0.j
        r.k = r0.k
        r.pn = r0.pn
        r.n = rs.avg('n', n=1)
        # average 를 다시 average
        r.avgret = rs.avg('avgret')
        # 가중평균의 평균
        r.wavgret = rs.avg('wavgret')
        c.insert(r, tname)


# 이제 위에서 구한것의 time series 평균을 내서 패턴을 살펴봅시다
#%%
with dbopen('db') as c:
    # rf 붙입시다
    # tables = ['momavg4', 'momavg10']
    tables = ['momavg4']
    for tname in tables:
        c.join(
            [tname, '*', 'yyyymm'],
            ['rf', 'rf', 'yyyymm'],
            pkeys='yyyymm, j, k, pn'
        )
    # excess return 계산하고
    for tname in tables:
        c.create(f" select *, avgret - rf * 100 as exavgret, wavgret - rf * 100 as exwavgret from {tname}")

#%%
# print
with dbopen('db') as c:
    tname = 'momavg4_kosdaq_included'
    col = 'exavgret'

    c.drop('result_mom1')
    for j in [3, 6, 9, 12]:
        for k in [3, 6, 9, 12]:
            rs = c.rows(tname, where=f"""
            j={j} and k={k}
            and yyyymm >= 200102 and yyyymm <= 201512
            """)
            r = Row()
            r.j = j
            r.k = k
            rss = list(rs.group('pn'))
            for i, rs1 in enumerate(rss[1:], 1):
                r['p' + str(i)] = rs1.avg(col, n=3)
            high, low = rss[-1], rss[1]
            v, tval = ttest(diff(high[col], low[col]), 3)
            r.hl = v
            r.hl_tval = tval
            r.avg = rss[0].avg(col, n=3)

            c.insert(r, 'result_mom1')
    c.to_csv('result_mom1')

#%%
# Jegadeesh and Titman 1997 table 7
with dbopen('db') as c:
    js = [3, 6, 9, 12]
    psize = 4
    c.drop('momavg_event_for_3years')
    for j in js:
        for rs in c.fetch("mom", roll=(37, 1, "yyyymm", True), where=f"""
        (mkt="kospi" or mkt="kosdaq") and mom={j} and yyyymm >= 200012
        """):
            fdate = rs[0].yyyymm
            rs0 = rs.where(lambda r: r.yyyymm == fdate)
            rs1 = rs.where(lambda r: r.yyyymm > fdate)
            rs0.numbering({'momret': psize})
            rs1.follow(rs0, 'id', 'pn_momret')
            for month, rs2 in enumerate(rs1.group('yyyymm'), 1):
                high = rs2.where(lambda r: r.pn_momret == psize)
                low = rs2.where(lambda r: r.pn_momret == 1)
                r = Row()
                r.j = j
                r.yyyymm = high[0].yyyymm
                r.month = month
                r.ewdiff = high.avg('ret') - low.avg('ret')
                r.vwdiff = high.avg('ret', 'size1') - low.avg('ret', 'size1')
                c.insert(r, 'momavg_event_for_3years')

#%%
# print out the previous results
with dbopen('db') as c:
    # avg them first
    j = 12
    diffret = 'vwdiff'

    c.drop('momavg_event_temp')
    chunks = []
    for rs in c.fetch('momavg_event_for_3years', group='month', where=f"""
    yyyymm >= 200101 and yyyymm <= 201512 and j={j}
    """):
        seq = rs[diffret]
        chunks.append(seq)

    c.drop('result_jt7')
    for i in range(1, 37):
        xs = chunks[:i]
        x1 = xs[-1]
        x2 = []
        for x in zip(*xs):
            x2.append(sum(x))
        r = Row()
        r.month = i
        val, tval = ttest(x1)
        r.ret = val
        r.ret_tval = tval

        val, tval = ttest(x2)
        r.cumret = val
        r.cumret_tval = tval
        c.insert(r, 'result_jt7')
    c.to_csv('result_jt7')

print('done')

# FF3, FF5 time series regression
#%%
# momavg1, append risk factors

with dbopen('db') as c:
    psize = 10
    tname = 'momavg' + str(psize) + '_kosdaq_included'
    ret = 'wavgret'
    ff5 = 'ff5vw'
    factors = ['mkt', 'smb', 'hml', 'rmw', 'cma']
    # factors = ['mkt', 'smb', 'hml']
    # factors = ['mkt']
    # factors = []

    # momavg high low
    c.drop('momhl')
    for rs in c.fetch(tname, group="yyyymm, j, k"):
        r = Row()
        r.yyyymm = rs[0].yyyymm
        r.j = rs[0].j
        r.k = rs[0].k
        r.ret = rs[-1][ret] - rs[1][ret]
        c.insert(r, 'momhl')
    c.join(
        ['momhl', '*', 'yyyymm'],
        [ff5, 'smb, hml, rmw, cma, mktret', 'yyyymm'],
        ['rf', 'rf', 'yyyymm'],
        name='momhl',
        pkeys='yyyymm, j, k'
    )

    c.create("""
    select *, mktret - rf * 100 as mkt
    from momhl
    """)

    c.drop('result_ff')
    def fn(xs):
        for x in xs:
            print("%.3f" % x, end=',')
        print()
    for rs in c.fetch("momhl", group="j, k", where="""
    yyyymm >= 200102 and yyyymm <= 201512
    """):
        if not factors:
            r = Row()
            r.j = rs[0].j
            r.k = rs[0].k
            val, tval = ttest(rs['ret'], n=3)
            r.val = val
            r.tval = tval
            c.insert(r, 'result_ff')

        else:
            r = Row()
            r.j = rs[0].j
            r.k = rs[0].k
            res = rs.ols("ret ~ " + '+'.join(factors))
            for p, v, t, pval in zip(['const'] + factors, res.params, res.tvalues, res.pvalues):
                r[p] = str(round(v, 3)) + stars(pval)
                r[p + '_tval'] = round(t, 3)
            # print(res.summary())
            c.insert(r, 'result_ff')
    c.to_csv('result_ff')



# Time for limit to arbitrage variables


# Time to compute IVOL
# Past 36 months monthly return market model risidul standard deviation
# only when there are 36 observations
# First we need to append market ret and rf to mdata1
#%%
with dbopen('db') as c:
    c.join(
        ['mdata1', '*', 'yyyymm, id'],
        ['ff5ew', 'mktret as ewmkt', 'yyyymm,'],
        ['ff5vw', 'mktret as vwmkt', 'yyyymm,'],
        ['rf', 'rf', 'yyyymm,'],
        name='mdata2',
        pkeys='yyyymm, id'
    )



# It takes some time
#%%
with dbopen('db') as c:
    c.drop('ivol')
    for rs in c.fetch('mdata2', group='id', where="""
    isnum(ret, ewmkt, vwmkt, size, prc)
    and size > 0 and prc > 0"""):
        for rs1 in rs.roll(36, 1, 'yyyymm'):
            if rs1.isconsec('yyyymm', '1 month', '%Y%m'):
                r = Row()
                r.yyyymm = rs1[-1].yyyymm
                r.id = rs1[0].id
                r.ivol1 = st.stdev(rs1.ols('ret ~ ewmkt').resid)
                r.ivol2 = st.stdev(rs1.ols('ret ~ vwmkt').resid)
                # TODO: CAPM later and some more
                c.insert(r, 'ivol')



# Now compute trading volumn for the past 12 month
#%%
with dbopen('db') as c:
    c.drop('tvol')
    for rs in c.fetch('mdata2', group='id', where="isnum(tvol)"):
        for rs1 in rs.roll(12, 1, 'yyyymm'):
            if rs1.isconsec('yyyymm', '1 month', '%Y%m'):
                r = Row()
                r.yyyymm = rs1[-1].yyyymm
                r.id = rs1[-1].id
                r.tvol = sum(r.tvol for r in rs1)
                c.insert(r, 'tvol')


# zero frequency and amihud illiquidity
# counts how many zero rets in a year

# 우선 ddata 를 로딩하고
#%%
with dbopen('db') as c:
    c.load('ddata.csv', fn=append_yyyymm)



#%%
# TODO: 200 개 이상인거 체크
with dbopen('db') as c:
    c.drop('amizero')
    for rs in c.fetch('ddata', group='id', where="""
    isnum(ret, tvol) and not (tvol=0 and ret !=0)
    """):
        for rs1 in rs.roll(12, 1, 'yyyymm'):
            rs_sample = Rows(x[0] for x in rs1.group('yyyymm'))
            # <----
            if len(rs1) >= 200 and rs_sample.isconsec('yyyymm', '1 month', '%Y%m'):
                r = Row()
                r.yyyymm = rs1[-1].yyyymm
                r.id = rs1[0].id
                r.zero = len(rs1.where('ret=0'))

                xs = []
                for r1 in rs1.where('tvol > 0'):
                    xs.append(abs(r1.ret) / r1.tvol)
                # There are cases where all tvol is zero
                r.illiq = st.mean(xs) if xs else ''
                c.insert(r, 'amizero')


# cvol
# stdev of (operating cash flow / total asset) for the past 4 years
#%%
with dbopen('db') as c :
    c.load('acc1.csv', fn=append_yyyymm)


def append_yyyymm2(r):
    r.yyyymm = dconv(r.date, "%d%b%Y", "%Y%m")
    return r


# cashflow data from BJ
#%%
with dbopen('db') as c:
    c.load('cashflow.csv', fn=append_yyyymm2)

# merge acc1 and cashflow
#%%
with dbopen('db') as c:
    c.join(
        ['cashflow', 'yyyymm, fcode as id, cashflow', 'yyyymm, fcode'],
        ['acc1', 'asset', 'yyyymm, id'],
        name='cashflow1',
        pkeys='yyyymm, id')

#%%
with dbopen('db') as c:
    c.drop('cvol')
    for rs in c.fetch('cashflow1', group='id', where="""
    isnum(cashflow, asset) and asset > 0 """):
        for rs1 in rs.roll(4, 1, 'yyyymm'):
            if rs1.isconsec('yyyymm', '1 year', '%Y%m'):
                xs = []
                for a, b in rs1['cashflow, asset']:
                    xs.append(a / b)
                r = Row()
                r.yyyymm = rs1[-1]['yyyymm']
                r.id = rs1[-1]['id']
                r.cvol = st.stdev(xs)
                c.insert(r, 'cvol')


# cvol is annual data so you should extend it
# say for 200112 -> 200112, 200201, 200202, ..., 200211
#%%
with dbopen('db') as c:
    c.drop('cvol1')
    for r in c.fetch('cvol'):
        for i in range(12):
            r1 = r.copy()
            r1.yyyymm = dmath(r.yyyymm, f'{i} months', '%Y%m')
            c.insert(r1, 'cvol1')


# prc and acov is already prepared in mdata2


# Merge them all
#%%
with dbopen('db') as c:
    c.join(
        ['mom', '*', 'yyyymm, id'],
        ['ivol', 'ivol1, ivol2'],
        ['tvol', 'tvol as tvol12'],
        ['amizero', 'illiq, zero'],
        ['cvol1', 'cvol'],
        name='dset',
        pkeys='mom, yyyymm, id'
    )




# 2way sort
#%%
with dbopen('db') as c:
    # larbs = ['ivol1', 'ivol2', 'tvol12', 'illiq', 'prc', 'zero', 'anal', 'cvol']
    larbs = ['size']
    js = [3, 6, 9, 12]
    ks = [3, 6, 9, 12]
    c.drop('dsetavg_temp')

    for larb, j, k, dep in product(larbs, js, ks, [True, False]):
        print(larb, j, k, dep)
        pn_larb = 'pn_' + larb
        for rs in c.fetch('dset', roll=(k + 2, 1, 'yyyymm', True), where=f"""
        mom={j} and mkt='kospi' and size > 0 and isnum({larb}, momret, ret)
        and yyyymm >= 200012 and yyyymm <= 201512
        """):
            fdate = rs[0].yyyymm
            # holding begins from here, with skipping 1 month
            begdate = dmath(fdate, '2 months', '%Y%m')
            rs0 = rs.where(f'yyyymm={fdate}')
            # rs1 could be empty, since you've set roll to longest
            rs1 = rs.where(f'yyyymm >= {begdate}')

            # Something different for anal
            chksize = zerochunks('anal', 4) if larb == 'anal' else 4
            rs0.numbering({larb: chksize, 'momret': 4}, dep)
            rs1.follow(rs0, 'id', ['pn_momret', pn_larb])

            def tempfn(rs, zero1, zero2):
                r = Row()
                r.larb = larb
                r.j = j
                r.k = k
                r.dep = dep
                r.yyyymm = rs[0].yyyymm
                r.pn_larb = 0 if zero1 == 0 else rs[0][pn_larb]
                r.pn_momret = 0 if zero2 == 0 else rs[0].pn_momret
                r.n = len(rs)
                r.ewret = rs.avg('ret')
                r.vwret = rs.avg('ret', 'size1')
                return r

            # compute average here
            for rs2 in rs1.isnum('pn_momret', pn_larb).group(['yyyymm', pn_larb, 'pn_momret']):
                c.insert(tempfn(rs2, True, True), 'dsetavg_temp')

            for rs2 in rs1.isnum('pn_momret', pn_larb).group(['yyyymm', pn_larb]):
                c.insert(tempfn(rs2, True, 0), 'dsetavg_temp')

            for rs2 in rs1.isnum('pn_momret', pn_larb).group(['yyyymm', 'pn_momret']):
                c.insert(tempfn(rs2, 0, True), 'dsetavg_temp')

            for rs2 in rs1.isnum('pn_momret', pn_larb).group(['yyyymm']):
                c.insert(tempfn(rs2, 0, 0), 'dsetavg_temp')

    # And because the the rolling over, most of them are overlapped,
    # so we should average them again
    c.drop('dsetavg')
    for rs in c.fetch('dsetavg_temp', group='larb, j, k, dep, yyyymm, pn_larb, pn_momret'):
        r = Row()
        r0 = rs[0]
        r.larb = r0.larb
        r.j = r0.j
        r.k = r0.k
        r.dep = r0.dep
        r.yyyymm = r0.yyyymm
        r.pn_larb = r0.pn_larb
        r.pn_momret = r0.pn_momret
        r.n = rs.avg('n', n=1)
        r.ewret = rs.avg('ewret')
        r.vwret = rs.avg('vwret')
        c.insert(r, 'dsetavg')




# Now we print them out


#%%
with dbopen('db') as c:
    c.drop('dsetavg')
    c.load('dsetavg.csv')
        
        
        
        
        
        
#%%
        
with dbopen('db') as c:
    c.join(
        ['dsetavg', '*', 'yyyymm'],
        ['rf', 'rf', 'yyyymm'],
        name='dsetavg1'
    )
    c.drop('dsetavg2')
    for r in c.fetch('dsetavg1'):
        r.exewret = r.ewret - r.rf * 100
        r.exvwret = r.vwret - r.rf * 100
        c.insert(r, 'dsetavg2')


#%%

with dbopen('db') as c:
    psize = 4
    ret = 'exvwret'

    def pick(rs, i, j):
        return rs.where(lambda r: r.pn_larb == i and r.pn_momret == j)

    c.drop('result_2way')
    for rs in c.fetch('dsetavg2', group='dep, j, k, larb'):
#        for i1, rs1 in enumerate(rs.group('pn_larb')):
#            r = Row()
#            r0 = rs1[0]
#            r.dep = r0.dep
#            r.j = r0.j
#            r.k = r0.k
#            r.larb = r0.larb
#            for i2 in range(psize + 1):
#                rs2 = pick(rs1, i1, i2)
#                seq = rs2[ret]
#                val, tval = ttest(seq)
#                r['p' + str(i2)] = round(st.mean(seq), 3)
#            high = pick(rs1, i1, psize)
#            low = pick(rs1, i1, 1)
#            val, tval = ttest(diff(high[ret], low[ret]))
#            r.diff = val
#            r.diff_tval = tval
#            c.insert(r, 'result_2way')
#            
            
        r = Row()
        r0 = rs[0]
        r.dep = r0.dep
        r.j = r0.j
        r.k = r0.k
        r.larb = r0.larb
        tvals = []
        for i2 in range(psize + 1):
            high = pick(rs, psize, i2)
            low = pick(rs, 1, i2)
            val, tval = ttest(diff(high[ret], low[ret]))
            r['p' + str(i2)] = f'{val};{tval}'

        hh = pick(rs, psize, psize)[ret]
        hl = pick(rs, psize, 1)[ret]
        lh = pick(rs, 1, psize)[ret]
        ll = pick(rs, 1, 1)[ret]
        val, tval = ttest(diff(diff(hh, hl), diff(lh, ll)))
        r.diff = val
        r.diff_tval = tval
        c.insert(r, 'result_2way')
    c.to_csv('result_2way')



#%%
# with dbopen('db') as c:
#     c.drop('dsetavg')
#     c.load('dsetavg.csv')


#%%
with dbopen('db') as c:
    c.drop('temp')
    for rs in c.fetch('dsetavg_temp', group='larb, j, k, dep, yyyymm, pn_larb, pn_momret'):
        r = Row()
        r0 = rs[0]
        r.larb = r0.larb
        r.j = r0.j
        r.k = r0.k
        r.dep = r0.dep
        r.yyyymm = r0.yyyymm
        r.pn_larb = r0.pn_larb
        r.pn_momret = r0.pn_momret
        r.n = rs.avg('n', n=1)
        r.n1 = len(rs)
        c.insert(r, 'temp')

#%%

#%%

with dbopen('db') as c:
    c.rename('result_2way', 'result_2way_ew')
#%%


with dbopen('db') as c:
    c.to_csv('result_2way_ew')
    c.to_csv('result_2way_vw')
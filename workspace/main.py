from sqlplus import *
from scipy.stats import ttest_1samp
import statistics as st 


def fnguide(fname, colnames, sheet=None, encoding='euc-kr'):
    rss = readxl(fname, sheet_name=sheet, encoding=encoding)
    for _ in range(8):
        next(rss)
    ids = [x[0] for x in grouper(next(rss)[1:], len(colnames))]
    for _ in range(5):
        next(rss)
    for rs in rss:
        date = rs[0]
        for id, vals in zip(ids, grouper(rs[1:], len(colnames))):
            r = Row()
            r.id = id 
            r.date = date 
            for c, v in zip(colnames, vals):
                r[c] = v
            yield r            


def mktfn(r):
    r.yyyymm = dmath(r.date,'%Y-%m-%d', '%Y-%m')
    if r.mkt == '유가증권시장':
        r.mkt = 'kospi'
    elif r.mkt == '코스닥':
        r.mkt = 'kosdaq'
    return r

def addmonth(x, n):
    return dmath(x, '%Y-%m', months=n)


def bhr(rs):
    result = 1
    for r in rs:
        result *= 1 + r.ret / 100.0
    return (result - 1) * 100.0


# mometum computation
# 상장이후 폐지 각각 6개월 제거, 가격 1000원 미만 제거
def compute_mom(rs):
    mom_periods = [3, 6, 9, 12]
    rs = rs.copy()
    begdate, enddate = rs[0].yyyymm, rs[-1].yyyymm
    for p in mom_periods:
        rs1 = rs[6:-6]
        for rs2 in rs1.overlap(p):
            # consecutive
            if addmonth(rs2[0].yyyymm, p - 1) == rs2[-1].yyyymm and \
               len(rs2) == p and len(rs2.where(lambda r: r.prc < 1000)) == 0:
                r0 = rs2[-1]
                r0.begdate = begdate 
                r0.enddate = enddate 
                r0.mom = p 
                r0.momret = bhr(rs2)
                yield r0 


def compute_aggtvol(rs):
    def compute(r0, n):
        a = sum(r.nb_ind for r in rs1[:n]) 
        b = sum(r.nb_indus for r in rs1[:n]) 
        c = sum(r.nb_foreign for r in rs1[:n]) 
        tot = (abs(a) + abs(b) + abs(c)) / 2
        r0['nb_ind' + str(n)] = a / tot 
        r0['nb_indus' + str(n)] = b / tot 
        r0['nb_foreign' + str(n)] = c / tot

    for rs1 in rs.overlap(12):
        # consecutive
        if addmonth(rs1[0].yyyymm, 11) == rs1[-1].yyyymm:
            r0 = Row()
            r0.id = rs1[0].id 
            r0.yyyymm = addmonth(rs1[0].yyyymm, -1)
            compute(r0, 3)
            compute(r0, 6)
            compute(r0, 9)
            compute(r0, 12)

            yield r0 



def compute_nbavg(rs):
    for i, rs1 in enumerate(rs.chunk(4), 1):
        r = Row()
        r.j = rs[0].mom
        r.yyyymm = rs[0].yyyymm
        r.nb_foreign3 = rs1.avg('nb_foreign3')
        r.nb_foreign6 = rs1.avg('nb_foreign6')
        r.nb_foreign9 = rs1.avg('nb_foreign9')
        r.nb_foreign12 = rs1.avg('nb_foreign12')

        r.nb_ind3 = rs1.avg('nb_ind3')
        r.nb_ind6 = rs1.avg('nb_ind6')
        r.nb_ind9 = rs1.avg('nb_ind9')
        r.nb_ind12 = rs1.avg('nb_ind12')

        r.nb_indus3 = rs1.avg('nb_indus3')
        r.nb_indus6 = rs1.avg('nb_indus6')
        r.nb_indus9 = rs1.avg('nb_indus9')
        r.nb_indus12 = rs1.avg('nb_indus12')

        r.pno = i
        yield r 



def compute_tvag_nbavg(rs):
    r = Row()
    r.j = rs[0].j 
    r.pno = rs[0].pno 
    r.nb_foreign3 = rs.avg('nb_foreign3', ndigits=4) 
    r.nb_foreign6 = rs.avg('nb_foreign6', ndigits=4) 
    r.nb_foreign9 = rs.avg('nb_foreign9', ndigits=4) 
    r.nb_foreign12 = rs.avg('nb_foreign12', ndigits=4) 

    r.nb_ind3 = rs.avg('nb_ind3', ndigits=4) 
    r.nb_ind6 = rs.avg('nb_ind6', ndigits=4) 
    r.nb_ind9 = rs.avg('nb_ind9', ndigits=4) 
    r.nb_ind12 = rs.avg('nb_ind12', ndigits=4) 

    r.nb_indus3 = rs.avg('nb_indus3', ndigits=4) 
    r.nb_indus6 = rs.avg('nb_indus6', ndigits=4) 
    r.nb_indus9 = rs.avg('nb_indus9', ndigits=4) 
    r.nb_indus12 = rs.avg('nb_indus12', ndigits=4) 
    return r



# momentum 포트폴리오 만들고 ret 계산 해보자 
def oneway(rs):
    rs = rs.where(lambda r: isnum(r.momret))
    rs.set('pn_momret', '')

    for i, rs1 in enumerate(rs.order('momret').chunk(4), 1):
        rs1.set('pn_momret', i)

    mom = rs[0].mom
    yyyymm = rs[0].yyyymm

    def build(rs1, k, i, pn_momret=False):
        r = Row()
        r.j = mom 
        r.k = k 
        r.yyyymm = addmonth(yyyymm, i)
        r.pn_momret = rs1[0].pn_momret if not pn_momret else 0 
        r.ewret = rs1.avg('r' + str(i))
        r.vwret = rs1.avg('r' + str(i), 's' + str(i))
        return r 

    for rs1 in rs.group('pn_momret'):
        # 첫달 빼고 3 개월 
        for i in range(2, 5):
            yield build(rs1, 3, i)
        for i in range(2, 8):
            yield build(rs1, 6, i)
        for i in range(2, 11):
            yield build(rs1, 9, i)
        for i in range(2, 14):
            yield build(rs1, 12, i)

    for i in range(2, 5):
        yield build(rs, 3, i, pn_momret=True)
    for i in range(2, 8):
        yield build(rs1, 6, i, pn_momret=True)
    for i in range(2, 11):
        yield build(rs1, 9, i, pn_momret=True)
    for i in range(2, 14):
        yield build(rs1, 12, i, pn_momret=True)


def zero_and_others(rs, col, n):
    zero = rs.where(lambda r: r[col] == 0)
    others = rs.where(lambda r: r[col] != 0).order(col).chunk(n)
    return [zero] + others


# 포트폴리오 만들고 return 까지 
def twoway(rs):
    # 우선 포트폴리오 만드는 것 부터 
    rs = rs.where(lambda r: isnum(r.frate, r.momret))
    rs.set('pn_frate', '')
    rs.set('pn_momret', '')

    for i, rs1 in enumerate(rs.order('frate').chunk(4), 1):
        rs1.set('pn_frate', i)
        for j, rs2 in enumerate(rs1.order('momret').chunk(4), 1):
            rs2.set('pn_momret', j)

    mom = rs[0].mom
    yyyymm = rs[0].yyyymm

    def build(rs1, k, i, pn_frate=False, pn_momret=False):
        r = Row()
        r.j = mom
        r.k = k
        r.yyyymm = addmonth(yyyymm, i)
        r.pn_frate = rs1[0].pn_frate if not pn_frate else 0 
        r.pn_momret = rs1[0].pn_momret if not pn_momret else 0 
        r.ewret = rs1.avg('r' + str(i))
        r.vwret = rs1.avg('r' + str(i), 's' + str(i))
        return r 
    
    for rs1 in rs.group('pn_frate, pn_momret'):
        for i in range(2, 5):
            yield build(rs1, 3, i)
        for i in range(2, 8):
            yield build(rs1, 6, i)
        for i in range(2, 11):
            yield build(rs1, 9, i)
        for i in range(2, 14):
            yield build(rs1, 12, i)

    for rs1 in rs.group('pn_frate'):
        for i in range(2, 5):
            yield build(rs1, 3, i, pn_momret=True)
        for i in range(2, 8):
            yield build(rs1, 6, i, pn_momret=True)
        for i in range(2, 11):
            yield build(rs1, 9, i, pn_momret=True)
        for i in range(2, 14):
            yield build(rs1, 12, i, pn_momret=True)

    for rs1 in rs.group('pn_momret'):
        for i in range(2, 5):
            yield build(rs1, 3, i, pn_frate=True)
        for i in range(2, 8):
            yield build(rs1, 6, i, pn_frate=True)
        for i in range(2, 11):
            yield build(rs1, 9, i, pn_frate=True)
        for i in range(2, 14):
            yield build(rs1, 12, i, pn_frate=True)

    for i in range(2, 5):
        yield build(rs, 3, i, pn_frate=True, pn_momret=True)
    for i in range(2, 8):
        yield build(rs1, 6, i, pn_frate=True, pn_momret=True)
    for i in range(2, 11):
        yield build(rs1, 9, i, pn_frate=True, pn_momret=True)
    for i in range(2, 14):
        yield build(rs1, 12, i, pn_frate=True, pn_momret=True)



def avgthem(rs):
    r = Row()
    r.j = rs[0].j 
    r.k = rs[0].k 
    r.yyyymm = rs[0].yyyymm 
    r.pn_frate = rs[0].pn_frate 
    r.pn_momret = rs[0].pn_momret
    r.ewret = rs.avg('ewret') 
    r.vwret = rs.avg('vwret') 
    return r

def avgthem1(rs):
    r = Row()
    r.j = rs[0].j 
    r.k = rs[0].k 
    r.yyyymm = rs[0].yyyymm 
    r.pn_momret = rs[0].pn_momret
    r.ewret = rs.avg('ewret') 
    r.vwret = rs.avg('vwret') 
    return r


def diff(high, low):
    return [a - b for a, b in zip(high, low)]

def stars(pval):
    if pval <= 0.01:
        return "***"
    elif pval <= 0.05:
        return "**"
    elif pval <= 0.10:
        return "*"
    return ""

def ttest(seq, n=3):
    tval, pval = ttest_1samp(seq, 0.0)
    return f'{round(st.mean(seq), n)}{stars(pval)}', round(tval, n)


def table1d(rs, m, pncol, retcol):
    def get(rs, i):
        return rs.where(lambda r: r[pncol] == i) 
    r = Row()
    for i in range(m + 1):
        r['p' + str(i)] = get(rs, i).avg(retcol, ndigits=3) 
    # diff 
    high = get(rs, m)[retcol]
    low = get(rs, 1)[retcol] 
    v, tval = ttest(diff(high, low))
    r.diff = v 
    r.tval = tval  
    yield r 


def table2d(rs, m, n, pncol1, pncol2, retcol):
    def get(rs, i, j):
        return rs.where(lambda r: r[pncol1] == i and r[pncol2] == j)
       
    for i in range(m + 1):
        r = Row(p=i)
        for j in range(n + 1):
            rs1 = get(rs, i, j)
            r['p' + str(j)] = rs1.avg(retcol, ndigits=3)
        high = get(rs, i, n)[retcol]
        low = get(rs, i, 1)[retcol]

        v, tval = ttest(diff(high, low))
        r.diff = v 
        r.tval = tval 
        yield r 
    # difference line
    r1 = Row(p='diff')
    r2 = Row(p='tval')
    for j in range(n + 1):
        high = get(rs, m, j)[retcol]
        low = get(rs, 1, j)[retcol]
        v, tval = ttest(diff(high, low)) 
        r1['p' + str(j)] = v 
        r2['p' + str(j)] = tval

    # diff of diff 
    hh = get(rs, m, n)[retcol]
    hl = get(rs, m, 1)[retcol]
    lh = get(rs, 1, n)[retcol]
    ll = get(rs, 1, 1)[retcol]

    v, tval = ttest(diff(diff(hh, hl), diff(lh, ll)))

    r1.diff = v 
    r1.tval = tval 
    r2.diff = '' 
    r2.tval = ''              
    yield r1 
    yield r2 

def result_1way(rs1):
    j = rs1[0].j
    k = rs1[0].k
    rs = Rows(table1d(rs1, 4, 'pn_momret', 'ewret'))
    rs.set('j', j) 
    rs.set('k', k) 
    rs.set('ret', 'ewret') 
    
    yield from rs

    rs = Rows(table1d(rs1, 4, 'pn_momret', 'vwret'))
    rs.set('j', j) 
    rs.set('k', k) 
    rs.set('ret', 'vwret') 
    yield from rs


def result_2way(rs1):
    j = rs1[0].j
    k = rs1[0].k
    rs = Rows(table2d(rs1, 4, 4, 'pn_frate', 'pn_momret', 'ewret'))

    rs.set('j', j) 
    rs.set('k', k) 
    rs.set('ret', 'ewret') 
    
    yield from rs

    rs = Rows(table2d(rs1, 4, 4, 'pn_frate', 'pn_momret', 'vwret'))
    rs.set('j', j) 
    rs.set('k', k) 
    rs.set('ret', 'vwret') 
    yield from rs


def rfac(rs):
    pncol1 = 'pn_frate'
    pncol2 = 'pn_momret'

    def get(rs, i, j):
        return rs.where(lambda r: r[pncol1] == i and r[pncol2] == j)

    def diff(high, low, col):
        rs = []
        for r1, r2 in zip(high, low):
            r1.dret = r1[col] - r2[col]
            rs.append(r1)
        return Rows(rs)

    high = get(rs, 4, 4)
    low = get(rs, 4, 1)
     
    rs1 = Rows(regtable(diff(high, low, 'ewret'), 'dret ~  mf'))
    rs1.set('j', high[0].j)
    rs1.set('k', high[0].k)
    yield from rs1
    yield emptyrow(rs1[0])

    rs1 = Rows(regtable(diff(high, low, 'ewret'), 'dret ~  mf + smb + hml'))
    rs1.set('j', high[0].j)
    rs1.set('k', high[0].k)
    yield from rs1
    yield emptyrow(rs1[0])

    rs1 = Rows(regtable(diff(high, low, 'ewret'), 'dret ~  mf + smb + hml + rmw + cma'))
    rs1.set('j', high[0].j)
    rs1.set('k', high[0].k)
    yield from rs1

    yield emptyrow(rs1[0])
    yield emptyrow(rs1[0])

    rs1 = Rows(regtable(diff(high, low, 'vwret'), 'dret ~  mfv'))
    rs1.set('j', high[0].j)
    rs1.set('k', high[0].k)
    yield from rs1
    yield emptyrow(rs1[0])

    rs1 = Rows(regtable(diff(high, low, 'vwret'), 'dret ~  mfv + smbv + hmlv'))
    rs1.set('j', high[0].j)
    rs1.set('k', high[0].k)
    yield from rs1
    yield emptyrow(rs1[0])

    rs1 = Rows(regtable(diff(high, low, 'vwret'), 'dret ~  mfv + smbv + hmlv + rmwv + cmav'))
    rs1.set('j', high[0].j)
    rs1.set('k', high[0].k)
    yield from rs1
    yield emptyrow(rs1[0])

    yield emptyrow(rs1[0])
    yield emptyrow(rs1[0])
    yield emptyrow(rs1[0])
    yield emptyrow(rs1[0])

def emptyrow(r):
    r = r.copy()
    for c in r.columns:
        r[c] = ''
    return r


def regtable(rs, model):
    result = rs.ols(model)
    depvar = model.split('~')[0].strip()
    for name, coef, tval, pval in zip(result.params.index, result.params.values, 
                                      result.tvalues, result.pvalues):
        r = Row()
        r.depvar = depvar
        r.col = name 
        r.coef = str(round(coef, 3)) + stars(pval)
        r.tval = round(tval, 3)
        r.pval = round(pval, 3)
        r.rsquared = round(result.rsquared, 3)
        r.nobs = result.nobs  

        yield r  

 
def rfac1(rs):
    pncol = 'pn_momret'

    def get(rs, i):
        return rs.where(lambda r: r[pncol] == i)

    def diff(high, low):
        rs = []
        for r1, r2 in zip(high, low):
            r1.dret = r1.ewret - r2.ewret
            rs.append(r1)
        return Rows(rs)

    j = rs[0].j
    k = rs[0].k

    high = get(rs, 4)
    low = get(rs, 1)

    rs = regtable(diff(high, low), 'dret ~ mf')
    yield from rs


def compute_rf(rs):
    def comp_rf(r0, r1):
        a = 1 / (1 + r0)
        b = 1 / (1 + r1) ** (11 / 12)
        return (b - a) * 100 / a

    r0, r1 = rs 
    r = Row()
    r.yyyymm = r1.yyyymm 
    r.rf = comp_rf(r0.rf / 100, r1.rf / 100)
    return r 

    
# mometum computation
# 상장이후 폐지 각각 6개월 제거, 가격 1000원 미만 제거
if __name__ == "__main__":
    yyyymm1 = {'date': lambda r: str(r.date)[0:10], 
               'yyyymm': lambda r: dmath(r.date, '%Y-%m-%d', '%Y-%m')}

    yyyymm = {'yyyymm': lambda r: dmath(r.date, '%Y-%m-%d', '%Y-%m')}
    yyyymm2 = {'yyyymm': lambda r: dmath(r.Date, '%Y-%m-%d', '%Y-%m'), 
               'mf': lambda r: r.mktret - r.rf}
               
    # drop('result_rfac')
    # drop('twoway2')

    process(
        Load(fnguide('manal.csv', ['anal']), name='manal', fn=yyyymm),
        Load(fnguide('mprc.csv', ['prc']), name='mprc', fn=yyyymm),
        Load(fnguide('mdata.csv', ['ret', 'size', 'tvol', 'equity', 'pref']), name='mdata', fn=yyyymm),
        Load('ff5_ew_mine.sas7bdat', 'ff5ew', fn=yyyymm2),
        Load('ff5_vw_mine.sas7bdat', 'ff5vw', fn=yyyymm2),
        Load(fnguide('indcode.csv', ['mkt', 'fname', 'icode']), name='indcode', fn=mktfn),

        Load(fnguide('fsize.xlsx', ['tsize', 'fsize']), name='fsize', fn=yyyymm1),
        Load(fnguide('ftvol.xlsx', ['nb_ind', 'nb_indus', 'nb_foreign']),
             name='ftvol', fn=yyyymm1),

        # Load(fnguide('ddata.csv', ['ret', 'size', 'tvol']),
        #      name='ddata', fn=yyyymm1),

        Load(fnguide('drfree.csv', ['rf']), name='drfree', fn=yyyymm1),


        # 무위험 수익률 계산하기 (rf)
        Map(lambda rs: rs[-1], 'drfree', group='yyyymm', order='date', 
            name='drfree_last_date_of_month', where=lambda r: isnum(r.rf)),
        
        Map(compute_rf, 'drfree_last_date_of_month', overlap=2, order='yyyymm',
            name='rf'),


        Join(
            ['mdata', '*', 'yyyymm, id'],
            ['mdata', 'size as size1', lambda r: (addmonth(r.yyyymm, 1), r.id)],
            ['indcode', 'mkt, icode', 'yyyymm, id'],
            ['manal', 'anal', 'yyyymm, id'],
            ['mprc', 'prc', 'yyyymm, id'],
            name='mdata1'
        ),

        Map(compute_mom, 'mdata1', name='mom', group='id', order='yyyymm', 
            where=lambda r: r.size > 0 and r.tvol >= 0),

        # ftvol 3, 6, 9, 12 개월로 계산해 봅시다 
        Map(compute_aggtvol, 'ftvol', name='ftvol1', group='id', order='yyyymm',
            where=lambda r: isnum(r.nb_ind, r.nb_indus, r.nb_foreign)),

        Join(
            ['mom', '*', 'id, yyyymm'],
            ['ftvol1', """
            nb_ind3, nb_indus3, nb_foreign3, nb_ind6, nb_indus6, nb_foreign6,
            nb_ind9, nb_indus9, nb_foreign9, nb_ind12, nb_indus12, nb_foreign12
            """, 'id, yyyymm'],
            name='mom1'
        ),

        Map(compute_nbavg, 'mom1', group='mom, yyyymm', order='momret',
            where=lambda r: (r.mkt == 'kospi' or r.mkt == 'kosdaq') and isnum(r.momret), 
            name='nbavg'),

        Map(compute_tvag_nbavg, 'nbavg', group='j, pno', 
            where=lambda r: r.yyyymm >= '2000-01' and r.yyyymm <= '2015-12',
            name='result_nbavg'),

        Map({'frate': lambda r: (r.fsize / r.tsize) if isnum(r.fsize) else 0}, 
            'fsize', name='fsize1'),

        # test 용으로 mdata1 랑 합해보자 
        Join(
            ['mdata1', '*', 'id, yyyymm'], 
            ['fsize1', 'tsize, fsize, frate', 'id, yyyymm'],
            name='temp_fsize'
        ),

        Map(lambda r: r, 'mom', name='mom2', 
            where=lambda r: r.yyyymm >= '1998-01'),

        Join(
            ['mom2', 'mom, id, yyyymm, momret, mkt', 'id, yyyymm'], 
            ['mdata1', 'ret, size1', 'id, yyyymm'], 
            ['fsize1', 'frate', 'id, yyyymm'],
            name='mdata2'
        ),

        # show  
        # Map(lambda r: print(r), 'mdata1', where=lambda r: r.prc <0, name='temp'),


        # two way sorting
        # 10분 넘게 걸림 
        Join(
            ['mdata2', '*', 'mom, id, yyyymm'],
            ['mdata2', 'size1 as s2, ret as r2', lambda r: (r.mom, r.id, addmonth(r.yyyymm, -2))],
            ['mdata2', 'size1 as s3, ret as r3', lambda r: (r.mom, r.id, addmonth(r.yyyymm, -3))],
            ['mdata2', 'size1 as s4, ret as r4', lambda r: (r.mom, r.id, addmonth(r.yyyymm, -4))],
            ['mdata2', 'size1 as s5, ret as r5', lambda r: (r.mom, r.id, addmonth(r.yyyymm, -5))],
            ['mdata2', 'size1 as s6, ret as r6', lambda r: (r.mom, r.id, addmonth(r.yyyymm, -6))],
            ['mdata2', 'size1 as s7, ret as r7', lambda r: (r.mom, r.id, addmonth(r.yyyymm, -7))],
            ['mdata2', 'size1 as s8, ret as r8', lambda r: (r.mom, r.id, addmonth(r.yyyymm, -8))],
            ['mdata2', 'size1 as s9, ret as r9', lambda r: (r.mom, r.id, addmonth(r.yyyymm, -9))],
            ['mdata2', 'size1 as s10, ret as r10', lambda r: (r.mom, r.id, addmonth(r.yyyymm, -10))],
            ['mdata2', 'size1 as s11, ret as r11', lambda r: (r.mom, r.id, addmonth(r.yyyymm, -11))],
            ['mdata2', 'size1 as s12, ret as r12', lambda r: (r.mom, r.id, addmonth(r.yyyymm, -12))],
            ['mdata2', 'size1 as s13, ret as r13', lambda r: (r.mom, r.id, addmonth(r.yyyymm, -13))],
            name='mdata3'
        ),

        # single sort 로 다시 한번 Jensen alpha 나 계산해고 같은지나 확인해보자 뭔가 이상해 

        Map(oneway, 'mdata3', group='mom, yyyymm', 
            where=lambda r: (r.mkt == 'kospi' or r.mkt == 'kosdaq'),
            name='oneway'), 
        Map(avgthem1, 'oneway', group='j, k, yyyymm, pn_momret', name='oneway1'), 

        Join(
            ['oneway1', '*', 'yyyymm'], 
            ['rf', 'rf', 'yyyymm'],
            name='oneway2'
        ),

        Map({'ewret': lambda r: r.ewret - r.rf, 'vwret': lambda r: r.vwret - r.rf},
            'oneway2', name='oneway3'),

        Map(result_1way, 'oneway3', group='j, k', name='result_1way', 
            where=lambda r: r.yyyymm >= '2001-02' and r.yyyymm <= '2015-12'),

        # Map(rfac1, 'oneway2', group='j, k', name='result_rfac',
        #     where=lambda r: r.yyyymm >= '2001-01' and r.yyyymm <= '2015-12'),

        Map(twoway, 'mdata3', group='mom, yyyymm', 
            where=lambda r: (r.mkt == 'kospi' or r.mkt == 'kosdaq'),
            name='twoway'), 
        # 겹치는 애들 다시 평균해주기 
        Map(avgthem, 'twoway', group='j, k, yyyymm, pn_frate, pn_momret', name='twoway1'),

        # 무위험 수익률 붙여주기 
        Join(
            ['twoway', '*', 'yyyymm'],
            ['rf', 'rf', 'yyyymm'],
            name='twoway2'
        ),
        Map({'ewret': lambda r: r.ewret - r.rf, 'vwret': lambda r: r.vwret - r.rf},
            'twoway2', name='twoway3'),

        # result02 
        Map(result_2way, 'twoway3', group='j, k', name='result_2way', 
            where=lambda r: r.yyyymm >= '2001-02' and r.yyyymm <= '2015-12'),
    
        # risk factor 로 설명 되는지 한번 살펴보자 
        # TODO: rf 새걸로 써야 해
        Join(
            ['twoway1', '*', 'yyyymm'],
            ['ff5ew', 'mf, smb, hml, rmw, cma', 'yyyymm'],
            ['ff5vw', 'mf as mfv, smb as smbv, hml as hmlv, rmw as rmwv, cma as cmav', 'yyyymm'],
            name='twoway4'
        ),

        Map(rfac, 'twoway4', group='j, k', name='result_rfac',
            where=lambda r: r.yyyymm >= '2001-02' and r.yyyymm <= '2015-12'),

        'Done'

    ) 
    # tocsv('result_nbavg')
    # tocsv('result_1way')
    # tocsv('result_2way')





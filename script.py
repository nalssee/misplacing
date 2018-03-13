"""You may want to exploit multicore cpus
"""

from itertools import product
from sqlplus import *

# setwd("C:\\Users\\kenjin\\WS\\mispricing")
setwd("/home/jinisrolling/WS/mispricing")


def zerochunks(col, n):
    def fn(rs):
        rs0 = rs.where(lambda r: r[col] == 0)
        rs1 = rs.where(lambda r: r[col] > 0)
        yield rs0
        yield from rs1.chunks(n - 1)
    return fn


def fn(rs, dep, j, k, larb, pn_larb):
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

    result = []
    # compute average here
    for rs2 in rs1.isnum('pn_momret', pn_larb).group(['yyyymm', pn_larb, 'pn_momret']):
        result.append(tempfn(rs2, True, True))

    for rs2 in rs1.isnum('pn_momret', pn_larb).group(['yyyymm', pn_larb]):
        result.append(tempfn(rs2, True, 0))

    for rs2 in rs1.isnum('pn_momret', pn_larb).group(['yyyymm', 'pn_momret']):
        result.append(tempfn(rs2, 0, True))

    for rs2 in rs1.isnum('pn_momret', pn_larb).group(['yyyymm']):
        result.append(tempfn(rs2, 0, 0))
    return Rows(result)


if __name__ == "__main__":
    with dbopen('db') as c:
        larbs = ['ivol1', 'ivol2', 'tvol12', 'illiq', 'prc', 'zero', 'anal', 'cvol']
        # larbs = ['ivol1', 'anal']
        js = [3, 6, 9, 12]
        ks = [3, 6, 9, 12]
        c.drop('dsetavg_temp')
        for larb, j, k, dep in product(larbs, js, ks, [True, False]):
            print(larb, j, k, dep)
            pn_larb = 'pn_' + larb
            for rs in pmap(fn, c.fetch('dset', roll=(k + 2, 1, 'yyyymm', True),
            cols=f"yyyymm, id, momret, size1, ret, {larb}",
            where=f"""
            mom={j} and (mkt='kospi' or mkt='kosdaq') and size > 0 and isnum({larb}, momret, ret)
            and yyyymm >= 200012 and yyyymm <= 201512
            """), args=(dep, j, k, larb, pn_larb), max_workers=8):
                c.insert(rs, 'dsetavg_temp')

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


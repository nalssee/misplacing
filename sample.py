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
        result *= 1 + r.ret / 100.0
    return (result - 1) * 100.0


def append_yyyymm(r):
    r.yyyymm = dconv(r.date, '%Y-%m-%d', '%Y-%m')
    return r



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


# portfolio numbering
def pnum(rs, col1, fn1, col2, fn2):
    for i, rs1 in enumerate(fn1(rs), 1):
        rs1.assign('pn_' + col1, i)
    for j, rs2 in enumerate(fn2(rs), 1):
        rs2.assign('pn_' + col2, j)


# dependent
def pnumd(rs, col1, fn1, col2, fn2):
    for i, rs1 in enumerate(fn1(rs), 1):
        rs1.assign('pn_' + col1, i)
        for j, rs2 in enumerate(fn2(rs1), 1):
            rs2.assign('pn_' + col2, j)


def nchunk(col, n):
    return lambda rs: rs.order(col).chunk(n)


def zerochunk(col, n):
    def fn(rs):
        rs0 = rs.where(lambda r: r[col] == 0)
        rs1 = rs.where(lambda r: r[col] > 0)
        return [rs0] + rs1.order(col).chunk(n-1)
    return fn


setdir("data")


# def func(db, mom_periods):
#     with connect(db) as c:
#         def mom():
#             for rs in c.fetch('mdata1', group='id', order='yyyymm', where="""
#             isnum(ret, size, tvol, prc) and
#             prc > 0 and size > 0 and tvol >= 0
#             """):
#                 begdate, enddate = rs[-1].date, rs[-1].date
#                 for p in mom_periods:
#                     # start and end 6 months cut
#                     # data span is 1980 and 2016.9
#                     # and since I am interested only on 1999 to 2015 the following
#                     # is about fine
#                     rs1 = rs[6:-6]
#                     for rs2 in rs1.overlap(p):
#                         if isconsec(rs2['yyyymm'], '1 month', '%Y-%m') and \
#                             len(rs2.where(lambda r: r.prc < 1000)) == 0 and len(rs2) == p:
#                                 # most recent one
#                                 r0 = rs2[-1]
#                                 r0.begdate = begdate
#                                 r0.enddate = enddate
#                                 r0.mom = p
#                                 r0.momret = bhr(rs2)
#                                 yield r0
#         c.insert(mom(), 'mom', pkeys="yyyymm, id, mom")

                # and yyyymm >= 200012 and yyyymm <= 201512
# dbfile = 'db.db'
# larbs = ['zero']

def func(dbfile, larbs):
    with connect(dbfile) as c:
        # larbs = ['size']
        js = [3, 6, 9, 12]
        ks = [3, 6, 9, 12]
        def dsetavg_temp():
            for larb, j, k, dep in product(larbs, js, ks, [True, False]):
                print(larb, j, k, dep)
                pn_larb = 'pn_' + larb
                for rs in c.fetch('dset', group='yyyymm', overlap=k + 2, where=f"""
                mom={j} and mkt='kospi' and size > 0 and isnum({larb}, momret, ret)
                and yyyymm >= 200012 and yyyymm <= 201512
                """):

                    fdate = rs[0].yyyymm
                    # holding begins from here, with skipping 1 month
                    begdate = dmath(fdate, '2 months', '%Y%m')
                    rs0 = rs.where(lambda r: r.yyyymm==fdate)
                    # rs1 could be empty, since you've set roll to longest
                    rs1 = rs.where(lambda r: r.yyyymm >= begdate)
                    # Something different for anal


                    chunkfn = zerochunk('anal', 4) if larb == 'anal' else nchunk(larb, 4)
                    if dep:
                        pnumd(rs0, larb, chunkfn, 'momret', nchunk('momret', 4))
                    else:
                        pnum(rs0, larb, chunkfn, 'momret', nchunk('momret', 4))

                    # rs1 follows rs0
                    rs1.assign('pn_momret', '')
                    rs1.assign(pn_larb, '')

                    for rsx in (rs0 + rs1).group('id'):
                        rsx.assign('pn_momret', rsx[0]['pn_momret'])
                        rsx.assign(pn_larb, rsx[0][pn_larb])

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
                        yield tempfn(rs2, True, True)

                    for rs2 in rs1.isnum('pn_momret', pn_larb).group(['yyyymm', pn_larb]):
                        yield tempfn(rs2, True, 0)

                    for rs2 in rs1.isnum('pn_momret', pn_larb).group(['yyyymm', 'pn_momret']):
                        yield tempfn(rs2, 0, True)

                    for rs2 in rs1.isnum('pn_momret', pn_larb).group(['yyyymm']):
                        yield tempfn(rs2, 0, 0)



        c.insert(dsetavg_temp(), 'dsetavg_temp')

        # And because the the rolling over, most of them are overlapped,
        # so we should average them again
        def dsetavg():
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
                r.n = rs.avg('n', ndigits=1)
                r.ewret = rs.avg('ewret')
                r.vwret = rs.avg('vwret')
                yield r

        c.insert(dsetavg(), 'dsetavg')


if __name__ == "__main__":

    with connect('db.db') as c:
        # c.pwork(func, 'dset', [[col] for col in ['ivol1', 'ivol2', 'tvol12', 'illiq', 'prc', 'zero', 'anal', 'cvol']])
        # c.pwork(func, 'dset', [[col] for col in ['ivol1', 'zero']])
        # c.load('dset.csv')
        # c.to_csv('dsetavg')
        pass
import math
import stemmer
import sqlite3

class TF_IDF:
    def __init__(self,k1,k3):
        self.porter = stemmer.Stemmer()
        self.idf_N = 0;
        self.icf_N = 0;
        self.k1 = k1;
        self.k3 = k3;
        self.b = 0.75;
          
    def word_tf(self, term_freq):
        return 1+math.log(term_freq*1.0)
        
    def word_idf(self, idf, term_doc_freq):
        return math.log(idf*1.0/term_doc_freq)

    def bm25(self,term_doc_freq,term_freq,ld,lavg):
        return math.log(self.idf_N*1.0/term_doc_freq)*(self.k1+1)*(term_freq)/(self.k1*(1-self.b)+self.b*(ld/lavg)+term_freq*1.0)*(self.k3+1)*term_freq/(self.k3+term_freq)
      
        
    def calc_sent_tfidf(self, sentence,lavg,lavglist):
        self.conn = sqlite3.connect("tfidf_full.db")
        
        cur = self.conn.cursor()
        cur.execute("select * from meta")
        self.idf_N = cur.fetchall()[0][0]

        cur = self.conn.cursor()
        cur.execute("select * from meta")
        self.icf_N = cur.fetchall()[0][1]
        
        
        doc_finder = "select * from docs where term = ?"
        term_finder = "select * from terms where term = ?"
        
        score_lst = {}
        ld = {}
        query = sentence.strip().split()
        for term in query:
            cur.execute(doc_finder, (term,))
            docs = cur.fetchall()
            if len(docs) == 0:
                continue
            cur.execute(term_finder, (term,))
            term_doc_freq = cur.fetchall()[0][1]
            
            for doc in docs:
                doc_id = doc[1]
                term_freq = doc[2]

                
            
                if doc_id in score_lst:
                    score_lst[doc_id] += self.bm25(term_doc_freq,term_freq,lavglist[doc_id],lavg)
                else:
                    score_lst[doc_id] = self.bm25(term_doc_freq,term_freq,lavglist[doc_id],lavg)
        
        self.conn.close();
        return score_lst
        
    def save_sorted_tfidf(self, save_path, query_file):
        save = open(save_path, 'w')
        start = 202
        querys = open(query_file)
        for query in querys:
            temp_query = []
            temp = self.porter.remove_symbol(query.lower()).replace("\n","").replace("\r","").split()
            for word in temp:
                temp_query.append(self.porter.stem(word,0,len(word)-1))
            temp_query = " ".join(temp_query)

            print start
            lavg , lavglist = self.get_Avr_L(temp_query)
            sc_lst = self.calc_sent_tfidf(temp_query,lavg,lavglist)
            sc_lst = sorted(sc_lst.items(), key=(lambda x:x[1]), reverse= True)
            
            for i, (doc, score) in enumerate(sc_lst[:1000]):
                save.write("%d Q%d %s %d %f %s\n"%(start, start-1, doc, i+1, score, "sodam"))
            start += 1
        save.close()
        querys.close();

    def get_Avr_L(self,sentence):
        self.conn = sqlite3.connect("tfidf_full.db")
        cur = self.conn.cursor()
        
        doc_finder = "select * from docs where term = ?"
        
        score_lst = {}
        query = sentence.strip().split()

        for term in query:
            cur.execute(doc_finder, (term,))
            docs = cur.fetchall()
            if len(docs) == 0:
                continue
            for doc in docs:
                doc_id = doc[1]
                term_freq = doc[2]
                
                if doc_id not in score_lst:
                    score_lst[doc_id] = {}
                    
                if term in score_lst[doc_id]:
                    score_lst[doc_id][term] += term_freq
                else:
                    score_lst[doc_id][term] = term_freq


                  
        lengthDocs = {}
        for docName in score_lst:
            
            for termName in score_lst[docName]:
                if docName in lengthDocs:
                    lengthDocs[docName] += score_lst[docName][termName]*score_lst[docName][termName]*1.0
                else:
                    lengthDocs[docName] = score_lst[docName][termName]*score_lst[docName][termName]*1.0
            lengthDocs[docName] = math.sqrt(lengthDocs[docName])
            
    
        
        sumdoclength = 0.0
        for docName2 in lengthDocs:
            sumdoclength += lengthDocs[docName2]

        self.conn.close();

        return sumdoclength/len(lengthDocs)*1.0 , lengthDocs
        
                
if __name__ == "__main__":
    klist = (1.6, 1.7)
    alist = (1.5, 1.6)
    for k in klist:
        for a in alist:
            scorer = TF_IDF(k,a)
            scorer.save_sorted_tfidf("testAP88_result%.1f-%.1f.txt"%(k,a),"topics.202-250.txt")

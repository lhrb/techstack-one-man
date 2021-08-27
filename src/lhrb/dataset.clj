(ns lhrb.dataset
  (:import (org.jsoup Jsoup)
           (org.jsoup.select Elements)
           (org.jsoup.nodes Element))
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.set :as set]
            [oz.core :as oz]))

(comment
 ;; use this to inspect jsoup results

 ;; eval in repl => strings will be shorten to 1000
 ;; without this my emacs will crash...
 ;; there is probably a better way doing this though
 (defn shorten-string [s]
   (if (> (count s) 1000)
     (str (.substring s 0 1000) " ...[" (count s)    "]")
     s))

 (defmethod print-method java.lang.String
   [v ^java.io.Writer w]
   (.write w (shorten-string  v)))
 ,)


;; valid words, query to reproduce the csv file from stack overflow tag + synonyms
;; https://data.stackexchange.com/stackoverflow/query/edit/1452594
;; https://stackoverflow.com/questions/33335492/list-of-all-tags-used-in-stackoverflow
#_ "
select
  t.Count,
  string_agg(TagSynonyms.SourceTagName, ',') as synonyms,
  t.tagName
from tags t
left join TagSynonyms
  on TagSynonyms.TargetTagName = t.tagName
group by t.tagName, t.Count
having t.Count > 5000
order by  t.Count desc
"

(defn read-csv [filename]
  (with-open [reader (io/reader filename)]
    (doall (csv/read-csv reader))))

(def valid-word
 (->> (read-csv "resources/QueryResults.csv")
      (rest) ;; drop csv head
      (map (fn [v] {:synonyms (v 1) :tag (v 2)}))
      (map (fn [m] (update m :synonyms #(str/split % #","))))
      (reduce
       (fn [acc-set elem]
         (-> acc-set
             (conj (:tag elem))
             (into (:synonyms elem))
             ))
       #{})))

(def common-english-words
  (into #{} (str/split-lines (slurp "resources/1-1000.txt"))))

(defn get-page [url]
  (.get (Jsoup/connect url)))

(defn get-elems [page css]
  (.select page css))

(defn words-from-page [url]
  (->> (get-elems (get-page url) ".comment")
       (map #(.text %))
       (map #(str/split % #"\s"))
       (flatten)))

(defn match-technologies [words]
  (->> words
       (map #(.toLowerCase %))
       (filter valid-word)
       (remove common-english-words)
       (remove empty?)
       (frequencies)
       (sort-by second)
       (reverse)))

(comment

;; url
;; 2021
;; https://news.ycombinator.com/item?id=28299053
;; 2020
;; https://news.ycombinator.com/item?id=25465582
;; 2019
;; https://news.ycombinator.com/item?id=21024041

  (def url "https://news.ycombinator.com/item?id=28299053")
  (def d2021
   (-> url
       (words-from-page)
       (match-technologies)))

  (def d2019
   (-> "https://news.ycombinator.com/item?id=21024041"
       (words-from-page)
       (match-technologies)))

  (oz/start-server!)

  (defn- word-cloud [wordcount]
  {:width 500
   :height 500
   :data [{:name "table"
           :values wordcount}]
   :scales [{:name "color"
             :type "ordinal"
             :domain {:data "table" :field "word"}
             :range {:scheme "reds"}}]
   :marks [{:type "text"
            :from {:data "table"}
            :encode {:enter {:text {:field "word"}
                             :align {:value "center"}
                             :baseline {:value "alphabetic"}
                             :fill {:scale "color" :field "word"}}
                     :update {:fillOpacity {:value 1}}
                     :hover {:fillOpacity {:value 0.5}}}
            :transform [{:type "wordcloud"
                         :size [500, 500]
                         :text {:field "word"}
                         :fontSize {:field "datum.count"}
                         :fontWeight "bold"
                         :fontSizeRange [12, 56]
                         :padding 2}]}]})


  (def word-cloud2019
    (word-cloud (map (fn [v] {:word (v 0) :count (v 1)}) d2019)))

  (def word-cloud2021
    (word-cloud (map (fn [v] {:word (v 0) :count (v 1)}) d2021)))


  (oz/view!
   word-cloud2019
   :mode
   :vega)

    (oz/view!
   word-cloud2021
   :mode
   :vega)

    ,)

(defn tech-unique-in-year [year & other-years]
  (let [convert-to-set      (fn [x] (into #{} (map first x)))
        tech-year           (convert-to-set year)
        tech-other-years    (map convert-to-set other-years)]
    (->> year
         (filter #((apply
                    set/difference
                    tech-year
                    tech-other-years)
                   (first %))))))

#_(tech-unique-in-year d2021 d2019)

(defn vectors-to-map [vectors]
  (reduce
   (fn [m [k v]]
     (assoc m k v))
   {}
   vectors))

(defn merge-years [y1 y2]
  (let [m-y1    (vectors-to-map y1)
        m-y2    (vectors-to-map y2)]
    (merge-with + m-y1 m-y2)))

(defn intersect [d2021 d2019]
  (let [tech-2021 (into #{} (map first d2021))
        tech-2019 (into #{} (map first d2019))]
    (->> (merge-years d2021 d2019)
         (reduce-kv (fn [m k v] (conj m [k v])) [])
         (filter #((set/intersection tech-2021 tech-2019) (first %)))
         (sort-by second)
         (reverse))))

#_(intersect d2021 d2019)

(defn md-tb-row [x]
  (str "|" (str/join "|" x) "|"))

(defn md-table [table]
  (let [table-header    (first table)
        content         (rest table)]
    (str
     (md-tb-row table-header)
     "\n"
     (md-tb-row (for [x table-header] " --- "))
     (->> content
          (map md-tb-row)
          (interleave (for [_ content] "\n"))
          (str/join)))))
(comment

  (defn tech+count-table [tb-data]
    (md-table (cons ["tech" "count"] tb-data)))

  (spit "resources/unique-2021.md"
        (tech+count-table (take 20 (tech-unique-in-year d2021 d2019))))
  (spit "resources/unique-2019.md"
        (tech+count-table (take 20 (tech-unique-in-year d2019 d2021))))
  (spit "resources/intersection-21-19.md"
        (tech+count-table (take 20 (intersect d2019 d2021))))


  ,)

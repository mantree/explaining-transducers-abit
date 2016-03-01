(ns transducers.core)

; Transducers

; What are they

; Transforming function (xf) that has control of when a Reducing function (rf) is called.


; Something to work with
(def nums (repeatedly 100 #(rand-int 20)))

; Transforming functions (xf) control what the Reducing function (rf) sees.

; Take filter
(def odd-filter (filter odd?))

; creates a filtering transducer, but what really is that. To the (trimmed, annotated) SOURCE!

(defn filter
  [pred]
  (fn [rf]
    (fn
      ; Initialize
      ([] (rf))
      ; Completion
      ([result] (rf result))
      ; Step
      ([result input]
       (if (pred input)
         (rf result input)
         result)))))

; This is what a Transforming function (xf) looks like. This one doesn't actually transform of course.
; Note how the predicate controls what the Reducing function (rf) gets called with.

; How do we use it?

; Transduce is used on finite collections

(transduce odd-filter conj [] nums)

; here we could actually just use into

(into [] odd-filter nums)

; eduction creates an iterable or reducible application of the xf. 
; applications are performed each time it is used!

(def educt (eduction odd-filter nums))

(reduce conj [] educt)
(take 5 educt)

; sequence gives a lazy result

(take 20 (sequence odd-filter (repeatedly #(rand-int 20))))


; Let's make a handy reducer, that fulfils the transducer contract.
; It's really just a reducer (two arity body), but with transducer arities to help it fit in.

(defn freqs
  ([] {})
  ; Step or Reducing function
  ([acc x]
   (update acc x 
           #(if %
              (inc %)
              1)))
  ([acc] acc))

; Identity is a handy xf for times like these

(transduce identity freqs nums)

; Equilivant reduce call, note reduce doesn't make use of the init function (or completion)
(reduce freqs {} nums)


; Let's find the most frequent number, introducing 'completing'

(def most-frequent
  (completing freqs
   #(first
     (reduce
      (fn [[_ mc :as cm] [_ c :as n]]
        (if (<= mc c)
          n cm))
      [nil 0] %))))

(transduce identity most-frequent nums)

; 'completing' adds or replaces the completion function of the given transducer or reducing function

; Let's do some actual transforming!
; and get some state into these things

; Count runs of odd numbers in the sequence

(defn odd-runs
  [rf]
  (let [run-c (atom 0)]
    (fn 
      ([] 
       (reset! run-c 0)
       (rf))
      ([acc x]   
       (let [c @run-c]         
         (if (and ((complement odd?) x)
                  (pos? c))
           (do
             (reset! run-c 0)
             (rf acc c))
           (do
             (swap! run-c inc)
             acc))))
      ([acc] 
       (rf acc)))))

(transduce odd-runs conj [] nums)

; what's our most frequent run?

(transduce odd-runs most-frequent nums)


; Let's create another aggregating (rf) function, average!

(defn safe-divide
  [d dd]
  (if (zero? dd)
    "0"
    (/ d dd)))

(defn average
  ([] {:count 0
       :sum 0})
  ([result input]
   (-> result
       (update :count inc)
       (update :sum + input)))
  ([result]
   (safe-divide
    (:sum result)
    (:count result))))

; Doesn't have any state, but is a reducing the stream into a completely different shape.

(transduce identity average nums)
(transduce odd-filter average nums)

; Transducers compose

(transduce (comp odd-filter
                 (map inc))
           most-frequent nums)

; What does transduce actually do? To the (trimmed, annotated) SOURCE!

(defn transduce
  ([xform f coll] 
   (transduce xform f (f) coll)) ; call init function of the reducing function (zero arity)
  ([xform f init coll]
     (let [f (xform f) ; bind the rf into the xf
           ;perform the transduction (?)
           ret (if (instance? clojure.lang.IReduceInit coll)
                 (.reduce ^clojure.lang.IReduceInit coll f init)
                 (clojure.core.protocols/coll-reduce coll f init))]
       (f ret)))) ; call completion function of the transducer

; so this is equilivent to the above
; relies on xf's zero arity calling down the rf's

(transduce identity 
           ((comp odd-filter
                  (map inc))
            most-frequent)
           nums)


; What if we want to compute our most-frequent and average at the same them in a single pass
; Huge props to Henry Garner for this nifty juxt-reducer, check out: https://skillsmatter.com/skillscasts/7243-expressive-parallel-analytics-with-clojure

(defn juxt-r
  [& rfns]
  (fn
    ([] (mapv 
         (fn [rf] (rf)) 
         rfns))
    ([results]
     (mapv 
      (fn [rf result] 
        (rf (unreduced result))) 
      rfns results))
    ([results input]
     (let [all-reduced? (volatile! true)
           results (mapv (fn [rf result]
                           (if-not (reduced? result)
                             (do (vreset! all-reduced? false)
                                 (rf result input))
                             result))
                         rfns results)]
       (if @all-reduced? (reduced results) results)))))

(transduce (comp odd-filter
                 (map inc))
           (juxt-r most-frequent
                   average)
           nums)

; Well isn't that marvellous

; Hey i thought the point was these things could work anywhere?!

; core.async/chan now accepts a transducer as an argument




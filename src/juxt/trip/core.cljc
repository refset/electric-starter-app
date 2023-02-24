(ns juxt.trip.core
  "Datalog as a namespace"
  (:require [clojure.spec.alpha :as s]
            [clojure.set :as set]
            #?@(:cljs [[cljs.js] [cljs.analyzer] [cljs.env]]))
  #?(:cljs (:require-macros [juxt.trip.core :refer [analyzer-state]])
     :clj (:import [clojure.lang Atom IAtom Indexed Symbol])))

#?(:bb (do)
   :clj (defrecord ^:no-doc Datom [e a v added]
          Indexed
          (nth [this n]
            (.nth this n nil))

          (nth [this n not-found]
            (case n
              0 (.e this)
              1 (.a this)
              2 (.v this)
              3 (.added this)
              not-found)))

   :cljs (defrecord ^:no-doc Datom [e a v added]
           IIndexed
           (-nth [this n]
             (-nth this n nil))

           (-nth [this n not-found]
             (case n
               0 (.-e this)
               1 (.-a this)
               2 (.-v this)
               3 (.-added this)
               not-found))))

(defprotocol DbConnection
  "Protocol representing a connection to a current immutable database value. Can commit transactions to transition to new states. See `-db` and `-commit`."
  (-db [this]
    "Returns the current `db` value for the connection. Part of the DbConnection protocol.")
  (-commit [this tx-report]
    "Commits a `tx-report` to the connection. Part of the DbConnection protocol."))

(defprotocol Db
  "Protocol representing an immutable database value. See `-datoms` and `-with`."
  (-datoms [this index components]
    "Returns the `datoms` in the db for a given index and prefix components. Part of the Db protocol.")
  (-with [this tx-data]
    "Returns a new updated db value. The tx-data is a list of `datoms`. Part of the Db protocol."))

(defn- logic-var? [x]
  (and (simple-symbol? x) (= \? (.charAt (name x) 0))))

(defn- source-var? [x]
  (and (simple-symbol? x) (= \$ (.charAt (name x) 0))))

(defn- rules-var? [x]
  (= '% x))

(defn- blank-var? [x]
  (= '_ x))

(s/def ::query (s/and (s/conformer identity vec)
                      (s/cat :find-spec ::find-spec
                             :return-map (s/? ::return-map)
                             :with-clause (s/? ::with-clause)
                             :inputs (s/? ::inputs)
                             :where-clauses (s/? ::where-clauses))))

(s/def ::find-spec (s/cat :find #{:find}
                          :find-spec (s/alt :find-rel ::find-rel
                                            :find-coll ::find-coll
                                            :find-tuple ::find-tuple
                                            :find-scalar ::find-scalar)))

(s/def ::return-map (s/alt :return-keys ::return-keys
                           :return-syms ::return-syms
                           :return-strs ::return-strs))

(s/def ::find-rel (s/+ ::find-elem))
(s/def ::find-coll (s/tuple ::find-elem '#{...}))
(s/def ::find-scalar (s/cat :find-elem ::find-elem :period '#{.}))
(s/def ::find-tuple (s/coll-of ::find-elem :min-count 1))

(s/def ::find-elem (s/or :variable ::variable :aggregate ::aggregate))

(s/def ::return-keys (s/cat :keys #{:keys} :symbols (s/+ symbol?)))
(s/def ::return-syms (s/cat :syms #{:syms} :symbols (s/+ symbol?)))
(s/def ::return-strs (s/cat :strs #{:strs} :symbols (s/+ symbol?)))

(s/def ::aggregate (s/cat :aggregate-fn-name ::plain-symbol :fn-args (s/+ ::fn-arg)))
(s/def ::fn-arg (s/or :variable ::variable :constant ::constant :src-var ::src-var))

(s/def ::with-clause (s/cat :with #{:with} :variables (s/+ ::variable)))
(s/def ::where-clauses (s/cat :where #{:where} :clauses (s/+ ::clause)))

(s/def ::inputs (s/cat :in #{:in} :inputs (s/+ (s/or :src-var ::src-var :binding ::binding :rules-var ::rules-var))))
(s/def ::src-var source-var?)
(s/def ::variable logic-var?)
(s/def ::rules-var rules-var?)
(s/def ::blank-var blank-var?)

(s/def ::plain-symbol (s/and symbol? (complement (some-fn source-var? logic-var? rules-var?))))

(s/def ::and-clause (s/cat :and '#{and} :clauses (s/+ ::clause)))
(s/def ::expression-clause (s/or :data-pattern ::data-pattern
                                 :pred-expr ::pred-expr
                                 :fn-expr ::fn-expr
                                 :rule-expr ::rule-expr))

(s/def ::rule-expr (s/cat :src-var (s/? ::src-var)
                          :rule-name ::rule-name
                          :args (s/+ (s/or :variable ::variable
                                           :constant ::constant
                                           :blank-var ::blank-var))))

(s/def ::not-clause (s/cat :src-var (s/? ::src-var)
                           :not '#{not}
                           :clauses (s/+ ::clause)))

(s/def ::not-join-clause (s/cat :src-var (s/? ::src-var)
                                :not-join '#{not-join}
                                :args (s/coll-of ::variable :kind vector? :min-count 1)
                                :clauses (s/+ ::clause)))

(s/def ::or-clause (s/cat :src-var (s/? ::src-var)
                          :or '#{or}
                          :clauses (s/+ (s/or :clause ::clause
                                              :and-clause ::and-clause))))

(s/def ::or-join-clause (s/cat :src-var (s/? ::src-var)
                               :or-join '#{or-join}
                               :args (s/and (s/conformer identity vec)
                                            ::rule-vars)
                               :clauses (s/+ (s/or :clause ::clause
                                                   :and-clause ::and-clause))))

(s/def ::rule-vars  (s/cat :bound-vars (s/? (s/coll-of ::variable :kind vector? :min-count 1))
                           :free-vars (s/* ::variable)))

(s/def ::clause (s/or :not-clause ::not-clause
                      :not-join-clause ::not-join-clause
                      :or-clause ::or-clause
                      :or-join-clause ::or-join-clause
                      :expression-clause ::expression-clause))

(s/def ::data-pattern (s/and (s/conformer identity vec)
                             (s/cat :src-var (s/? ::src-var)
                                    :pattern (s/+ (s/or :variable ::variable
                                                        :constant ::constant
                                                        :blank-var ::blank-var)))))

(s/def ::constant (complement (some-fn symbol? list?)))

(s/def ::pred-expr (s/tuple (s/cat :pred ::pred
                                   :args (s/* ::fn-arg))))
(s/def ::pred (s/or :symbol ::plain-symbol
                    :variable ::variable))

(s/def ::fn-expr (s/tuple (s/cat :fn ::fn
                                 :args (s/* ::fn-arg))
                          ::binding))
(s/def ::fn (s/or :symbol ::plain-symbol
                  :variable ::variable))

(s/def ::binding (s/or :bind-scalar ::bind-scalar
                       :bind-tuple ::bind-tuple
                       :bind-coll ::bind-coll
                       :bind-rel ::bind-rel))

(s/def ::bind-scalar (s/or :variable ::variable
                           :blank-var ::blank-var))
(s/def ::bind-tuple (s/coll-of (s/or :variable ::variable
                                     :blank-var ::blank-var)
                               :kind vector? :min-count 1))
(s/def ::bind-coll (s/tuple ::variable '#{...}))
(s/def ::bind-rel (s/tuple (s/coll-of (s/or :variable ::variable
                                            :blank-var ::blank-var)
                                      :kind vector? :min-count 1)))

(s/def ::rule (s/coll-of
               (s/and (s/conformer identity vec)
                      (s/cat :rule-head ::rule-head
                             :clauses (s/+ ::clause)))
               :kind vector?
               :min-count 1))
(s/def ::rule-head (s/and list? (s/cat :rule-name ::rule-name
                                       :rule-vars ::rule-vars)))
(s/def ::unqualified-plain-symbol simple-symbol?)
(s/def ::rule-name (s/and ::unqualified-plain-symbol (complement '#{and or or-join not not-join})))

;; Query runtime.

(declare datoms)

(defn ^:no-doc unbound-var? [x]
  (and (identical? Symbol (type x))
       (= \! (.charAt (name x) 0))))

(defn ^:no-doc data-pattern [$ e a v]
  (if (unbound-var? e)
    (if (unbound-var? a)
      (if (unbound-var? v)
        (datoms $ :aev)
        (datoms $ :vae v))
      (if (unbound-var? v)
        (datoms $ :aev a)
        (datoms $ :ave a v)))
    (if (unbound-var? a)
      (if (unbound-var? v)
        (datoms $ :eav e)
        (for [datom (datoms $ :eav e)
              :when (= v (:v datom))]
          datom))
      (if (unbound-var? v)
        (datoms $ :eav e a)
        (datoms $ :eav e a v)))))

(defn- ->unbound-var->position-var-smap [args]
  (loop [idx 0
         acc {}]
    (if (= (count args) idx)
      acc
      (let [arg (nth args idx)]
        (recur (inc idx) (if (unbound-var? arg)
                           (assoc acc arg (keyword "juxt.trip.core" (str "unbound-var-" idx)))
                           acc))))))

(defn ^:no-doc rule [db {:keys [memo-table rec-set] :as rule-table} rule-call-site leg-fns args]
  (let [unbound-var->position-var (->unbound-var->position-var-smap args)
        position-vars (set (vals unbound-var->position-var))
        rule-memo-key [leg-fns (replace unbound-var->position-var args)]
        memo (get @memo-table rule-memo-key ::not-found)]
    (if (= ::not-found memo)
      (let [rec-key [rule-call-site position-vars]
            rule-table (update rule-table :rec-set conj rec-key)]
        (loop [acc #{}]
          (swap! memo-table assoc rule-memo-key acc)
          (let [new-acc (set (for [leg-fn leg-fns
                                   x (apply leg-fn db rule-table args)]
                               x))]
            (if (set/subset? new-acc acc)
              (do (when (contains? rec-set rec-key)
                    (swap! memo-table dissoc rule-memo-key))
                  acc)
              (recur new-acc)))))
      memo)))

(def ^:private aggregate-fn-name->built-in-fn-name
  {'sum `agg-sum 'avg `agg-avg 'min `agg-min 'max `agg-max
   'count `agg-count-all 'count-distinct `agg-count-distinct
   'median `agg-median 'stddev `agg-stddev 'variance `agg-variance
   'distinct `agg-distinct 'rand `agg-rand 'sample `agg-sample
   'aggregate `agg-aggregate})

(defn ^:no-doc agg-sum [vals]
  (reduce + vals))

(defn ^:no-doc agg-avg [vals]
  (/ (agg-sum vals) (count vals)))

(defn ^:no-doc agg-min
  ([vals]
   (first (agg-min 1 vals)))
  ([n vals]
   (take n (sort vals))))

(defn ^:no-doc agg-max
  ([vals]
   (first (agg-max 1 vals)))
  ([n vals]
   (take-last n (sort vals))))

(defn ^:no-doc agg-count-all [vals]
  (count vals))

(defn ^:no-doc agg-count-distinct [vals]
  (count (distinct vals)))

(defn ^:no-doc agg-median [vals]
  (let [vals (sort vals)
        idx (bit-shift-right (count vals) 1)]
    (if (even? (count vals))
      (/ (+ (nth vals idx) (nth vals (dec idx))) 2)
      (nth vals idx))))

(defn ^:no-doc agg-variance [vals]
  (let [avg (agg-avg vals)]
    (/ (agg-sum (for [x vals
                      :let [d (- x avg)]]
                  (* d d)))
       (count vals))))

(defn ^:no-doc agg-stddev [vals]
  (#?(:clj Math/sqrt :cljs js/Math.sqrt) (agg-variance vals)))

(defn ^:no-doc agg-distinct [vals]
  (distinct vals))

(defn- ^:no-doc agg-rand [n coll]
  (vec (repeatedly n #(rand-nth coll))))

(defn- ^:no-doc agg-sample [n coll]
  (vec (take n (shuffle coll))))

(defn ^:no-doc agg-aggregate [aggregate vals]
  (aggregate vals))

(def ^:private fn-name->built-in-fn-name
  {'< `cmp-< '<= `cmp-<= '> `cmp-> '>= `cmp->= '!= `not=
   'ground `identity 'tuple `vector 'untuple `identity
   'get-else `get-else 'get-some `get-some 'missing? `missing?})

(defn- cmp-varargs [cmp x y more]
  (if (cmp x y)
    (if (next more)
      (recur cmp y (first more) (next more))
      (cmp y (first more)))
    false))

(defn- cmp [x y]
  (try
    (compare x y)
    (catch #?(:clj Exception :cljs :default) _
      (compare (str (type x)) (str (type y))))))

(defn ^:no-doc cmp-<
  ([x] x)
  ([x y]
   (neg? (cmp x y)))
  ([x y & more]
   (cmp-varargs cmp-< x y more)))

(defn ^:no-doc cmp-<=
  ([x] x)
  ([x y]
   (not (pos? (cmp x y))))
  ([x y & more]
   (cmp-varargs cmp-<= x y more)))

(defn ^:no-doc cmp->
  ([x] x)
  ([x y]
   (pos? (cmp x y)))
  ([x y & more]
   (cmp-varargs cmp-> x y more)))

(defn ^:no-doc cmp->=
  ([x] x)
  ([x y]
   (not (neg? (cmp x y))))
  ([x y & more]
   (cmp-varargs cmp->= x y more)))

(defn ^:no-doc get-else [src-var ent attr default]
  (if-let [vs (seq (datoms src-var :eav ent attr))]
    (let [[e a v] (first vs)]
      v)
    default))

(defn ^:no-doc get-some [src-var ent attr & attrs]
  (reduce
   (fn [_ attr]
     (let [v (get-else src-var ent attr ::not-found)]
       (when-not (= ::not-found v)
         (reduced [attr v]))))
   nil
   (cons attr attrs)))

(defn ^:no-doc missing? [src-var ent attr]
  (empty? (datoms src-var :eav ent attr)))

;; Query compiler.

(declare eval-internal)

(defn- ->unbound-var []
  (gensym '!unbound))

(defn- lvar-ref [x {:keys [vars free-rule-vars] :as ctx}]
  (cond
    (and (logic-var? x)
         (or (contains? vars x)
             (contains? free-rule-vars x)))
    x

    (or (logic-var? x)
        (blank-var? x))
    (list 'quote (->unbound-var))

    :else
    x))

(defn- all-vars [x]
  (set (filter logic-var? (distinct (tree-seq counted? seq x)))))

(defmulti ^:private datalog->clj (fn [datalog ctx] (first datalog)))

(defmulti ^:private analyze-datalog (fn [datalog ctx] (first datalog)))

(defn- with-lvar-refs [binding ctx]
  (if (vector? binding)
    (mapv #(with-lvar-refs % ctx) binding)
    (lvar-ref binding ctx)))

(defn- unify-groups [binding]
  (vec (for [[k unify-group] (group-by second (map-indexed vector binding))
             :when (and (logic-var? k) (> (count unify-group) 1))]
         (mapv first unify-group))))

(defn- unify->clj [binding-form binding {:keys [vars free-rule-vars] :as ctx}]
  (cond
    (sequential? binding)
    `(and ~@(->> (for [[idx binding] (map-indexed vector binding)]
                   (unify->clj `(nth ~binding-form ~idx) binding ctx))
                 (remove true?))
          ~@(when-let [unify-groups (not-empty (unify-groups binding))]
              `(~@(for [unify-group-idxs unify-groups]
                    `(= ~@(for [unify-group-idx unify-group-idxs]
                            `(nth ~binding-form ~unify-group-idx)))))))

    (blank-var? binding)
    true

    (or (not (logic-var? binding))
        (and (contains? vars binding)
             (not (contains? free-rule-vars binding))))
    `(= ~binding-form ~binding)

    (contains? free-rule-vars binding)
    `(or (juxt.trip.core/unbound-var? ~binding)
         (= ~binding-form ~binding))

    (logic-var? binding)
    true))

(defn- wrap-with-binding [[binding-type binding] form ctx]
  (let [binding-sym (gensym '__binding)]
    (case binding-type
      :bind-scalar (let [binding (second binding)]
                     `[:let [~binding-sym ~form]
                       :when ~(unify->clj binding-sym binding ctx)
                       :let [~binding ~binding-sym]])
      :bind-tuple (let [binding (mapv second binding)]
                    `[:let [~binding-sym ~form]
                      :when ~(unify->clj binding-sym binding ctx)
                      :let [~binding ~binding-sym]])
      :bind-coll (let [binding (first binding)]
                   `[~binding-sym ~form
                     :when ~(unify->clj binding-sym binding ctx)
                     :let [~binding ~binding-sym]])
      :bind-rel (let [binding (mapv second (first binding))]
                  `[~binding-sym ~form
                    :when ~(unify->clj binding-sym binding ctx)
                    :let [~binding ~binding-sym]]))))

(defn- pattern->bind-rel [pattern]
  [:bind-rel [(vec (for [pattern pattern]
                     (if (logic-var? pattern)
                       [:variable pattern]
                       [:blank-var '_])))]])

(defn- clauses->clj [ctx clauses projection]
  (loop [clauses clauses
         {:keys [vars] :or {vars #{}} :as ctx} ctx
         acc []]
    (if (empty? clauses)
      `(for [_# [nil] ~@acc]
         ~projection)
      (if-let [[clause {:keys [out-vars]}] (->> (for [clause clauses
                                                      :let [{:keys [in-vars] :as clause-vars} (analyze-datalog clause ctx)]
                                                      :when (set/subset? in-vars vars)]
                                                  [clause clause-vars])
                                                (first))]
        (recur (remove #{clause} clauses)
               (update ctx :vars set/union out-vars)
               (into acc (datalog->clj clause ctx)))
        (throw (ex-info (str "Cannot resolve dependencies for: " (pr-str clauses)) {}))))))

(defmethod datalog->clj :expression-clause [[_ clause] ctx]
  (datalog->clj clause ctx))

(defmethod analyze-datalog :expression-clause [[_ clause] ctx]
  (analyze-datalog clause ctx))

(defmethod datalog->clj :data-pattern [[_ {:keys [src-var pattern]}] {{:keys [src-sym]} :symbols :as ctx}]
  (let [pattern (vec (take 3 (concat (map second pattern) (repeat '_))))]
    (wrap-with-binding
     (pattern->bind-rel pattern)
     `(data-pattern ~(or src-var src-sym) ~@(with-lvar-refs pattern ctx))
     ctx)))

(defmethod analyze-datalog :data-pattern [[_ {:keys [pattern]}] ctx]
  {:out-vars (all-vars pattern)})

(defn- rule-args [rule-name args name->rules]
  (let [args (map second args)
        [bound-vars free-vars] (map vec (-> name->rules
                                            (get-in [rule-name 0 :rule-head :rule-vars :bound-vars])
                                            (count)
                                            (split-at args)))]
    {:bound-vars bound-vars :free-vars free-vars}))

(defmethod datalog->clj :rule-expr [[_ {:keys [src-var rule-name args]}] {:keys [name->rules] {:keys [rule-table-sym src-sym]} :symbols :as ctx}]
  (let [{:keys [bound-vars free-vars]} (rule-args rule-name args name->rules)]
    (wrap-with-binding
     (pattern->bind-rel free-vars)
     `(~rule-name ~(or src-var src-sym) ~rule-table-sym '~(gensym '__rule-call-site) ~@bound-vars ~@(with-lvar-refs free-vars ctx))
     ctx)))

(defmethod analyze-datalog :rule-expr [[_ {:keys [rule-name args]}] {:keys [name->rules] :as ctx}]
  (let [{:keys [bound-vars free-vars]} (rule-args rule-name args name->rules)]
    {:out-vars (all-vars free-vars)
     :in-vars (all-vars bound-vars)}))

(defmethod datalog->clj :pred-expr [[_ [{:keys [pred args]}]] _]
  (let [pred (second pred)]
    [:when `(~(get fn-name->built-in-fn-name pred pred) ~@(map second args))]))

(defmethod analyze-datalog :pred-expr [[_ [{:keys [pred args]}]] _]
  {:in-vars (all-vars (vec (cons pred args)))})

(defmethod datalog->clj :fn-expr [[_ [{:keys [fn args]} binding]] ctx]
  (let [fn (second fn)]
    (wrap-with-binding binding `(~(get fn-name->built-in-fn-name fn fn) ~@(map second args)) ctx)))

(defmethod analyze-datalog :fn-expr [[_ [{:keys [fn args]} binding]] _]
  {:in-vars (all-vars (vec (cons fn args)))
   :out-vars (all-vars binding)})

(defmethod datalog->clj :not-clause [[_ {:keys [src-var clauses]} :as not-clause] {{:keys [src-sym]} :symbols :as ctx}]
  (let [{:keys [in-vars]} (analyze-datalog not-clause ctx)]
    `[:when (let [~src-sym ~(or src-var src-sym)]
              (empty? ~(clauses->clj ctx clauses true)))]))

(defmethod analyze-datalog :not-clause [[_ {:keys [clauses]}] ctx]
  {:in-vars (->> (for [clause clauses
                       :let [{:keys [in-vars out-vars]} (analyze-datalog clause ctx)]]
                   (set/union in-vars out-vars))
                 (reduce into #{}))})

(defmethod datalog->clj :not-join-clause [[_ {:keys [src-var clauses args]}]  {{:keys [src-sym]} :symbols :as ctx}]
  (let [vars (set args)
        ctx (-> ctx
                (update :free-rule-vars set/intersection vars)
                (assoc :vars vars))]
    `[:when (let [~src-sym ~(or src-var src-sym)]
              (empty? ~(clauses->clj ctx clauses true)))]))

(defmethod analyze-datalog :not-join-clause [[_ {:keys [args]}] ctx]
  {:in-vars (all-vars args)})

(defmethod datalog->clj :or-clause [[_ {:keys [src-var clauses]} :as or-clause]  {{:keys [src-sym]} :symbols :as ctx}]
  (let [{:keys [in-vars out-vars]} (analyze-datalog or-clause ctx)
        ctx (update ctx :vars set/intersection (set/union in-vars out-vars))
        free-vars (vec out-vars)]
    (wrap-with-binding
     (pattern->bind-rel free-vars)
     `(let [~src-sym ~(or src-var src-sym)]
        (concat ~@(for [[clause-type clause] clauses]
                    (clauses->clj ctx
                                  (case clause-type
                                    :clause [clause]
                                    :and-clause (:clauses clause))
                                  free-vars))))
     ctx)))

(defmethod analyze-datalog :or-clause [[_ {:keys [clauses]}] ctx]
  (let [clause-vars (for [[clause-type clause] clauses]
                      (->> (for [clause (case clause-type
                                          :clause [clause]
                                          :and-clause (:clauses clause))]
                             (analyze-datalog clause ctx))
                           (apply merge-with into)))
        {:keys [in-vars out-vars]} (apply merge-with into clause-vars)]
    (when-not (apply = (for [{:keys [out-vars]} clause-vars]
                         (set/difference out-vars (:vars ctx))))
      (throw (ex-info (str "Or requires same vars: " (pr-str clause-vars)) {})))
    {:in-vars (set/difference in-vars out-vars)
     :out-vars (set/difference out-vars in-vars)}))

(defmethod datalog->clj :or-join-clause [[_ {:keys [src-var clauses] {:keys [bound-vars free-vars]} :args}]  {:keys [name->rules] {:keys [src-sym]} :symbols :as ctx}]
  (let [out-vars (set free-vars)
        ctx (-> ctx
                (update :free-rule-vars set/intersection out-vars)
                (update :vars set/intersection out-vars)
                (update :vars set/union (set bound-vars)))]
    (wrap-with-binding
     (pattern->bind-rel free-vars)
     `(let [~src-sym ~(or src-var src-sym)]
        (concat ~@(for [[clause-type clause] clauses]
                    (clauses->clj ctx
                                  (case clause-type
                                    :clause [clause]
                                    :and-clause (:clauses clause))
                                  free-vars))))
     ctx)))

(defmethod analyze-datalog :or-join-clause [[_ {{:keys [bound-vars free-vars]} :args}] ctx]
  {:in-vars (all-vars bound-vars)
   :out-vars (all-vars free-vars)})

(defmethod datalog->clj :inputs [[_ {:keys [inputs]}] {{:keys [inputs-sym]} :symbols :as ctx}]
  (->> (for [[idx [in-type in]] (map-indexed vector inputs)
             :when (= :binding in-type)]
         (wrap-with-binding in `(nth ~inputs-sym ~idx) ctx))
       (reduce into [])))

(defmethod analyze-datalog :inputs [[_ {:keys [inputs]}] ctx]
  {:out-vars (all-vars inputs)})

(defn- rule-leg-name [rule-name idx]
  (symbol (str rule-name "__" idx)))

(defn- rules->clj [{:keys [name->rules] {:keys [rule-table-sym src-sym]} :symbols :as ctx}]
  (->> (for [[rule-name rule-legs] name->rules
             :let [idx->rule-leg (zipmap (range (count rule-legs)) rule-legs)
                   rule-leg-refs (mapv #(rule-leg-name rule-name %) (keys idx->rule-leg))]]
         (->> (for [[idx {:keys [clauses]
                          {{:keys [bound-vars free-vars]} :rule-vars} :rule-head}] idx->rule-leg
                    :let [arg-vars (concat bound-vars free-vars)
                          ctx (assoc ctx
                                     :free-rule-vars (set free-vars)
                                     :vars (set bound-vars))]]
                `(~(rule-leg-name rule-name idx) [~src-sym ~rule-table-sym ~@arg-vars]
                  ~(clauses->clj ctx clauses (vec free-vars))))
              (cons `(~rule-name [db# rule-table# rule-call-site# & args#]
                      (rule db# rule-table# rule-call-site# ~rule-leg-refs args#)))))
       (reduce into [])))

(defn- aggregates->clj [find-elements {{:keys [group-sym]} :symbols :as ctx}]
  (for [[idx [find-elem-type find-elem]] (map-indexed vector find-elements)]
    (case find-elem-type
      :variable find-elem
      :aggregate (let [{:keys [aggregate-fn-name fn-args]} find-elem]
                   `(~(get aggregate-fn-name->built-in-fn-name aggregate-fn-name aggregate-fn-name)
                     ~@(map second (butlast fn-args)) (map #(nth % ~idx) ~group-sym))))))

(defn- find-spec-elements [find-spec]
  (let [[find-type find-spec] (:find-spec find-spec)]
    (case find-type
      (:find-rel :find-tuple) find-spec
      :find-coll [(first find-spec)]
      :find-scalar [(:find-elem find-spec)])))

(defn- find-element-projection [[find-elem-type find-elem]]
  (case find-elem-type
    :variable find-elem
    :aggregate (second (last (:fn-args find-elem)))))

(defn- group-idxs [find-elements]
  (vec (for [[idx [find-elem-type _]] (map-indexed vector find-elements)
             :when (= :variable find-elem-type)]
         idx)))

(defn- wrap-with-find-spec [find-spec {{:keys [group-sym]} :symbols :as ctx} clauses]
  (let [find-elements (find-spec-elements find-spec)
        aggregates? (some #{:aggregate} (map first find-elements))
        projected-vars (mapv find-element-projection find-elements)
        form (clauses->clj ctx clauses projected-vars)]
    (if aggregates?
      `(->> ~form
            (group-by #(mapv % ~(group-idxs find-elements)))
            (map (fn [[~projected-vars ~group-sym]]
                   [~@(aggregates->clj find-elements ctx)])))
      form)))

(defn- wrap-with-return-map [return-map _ query-form]
  (if-let [tuple-keys (when-let [[return-map-type {:keys [symbols]}] return-map]
                        (not-empty (map (case return-map-type
                                          :return-keys keyword
                                          :return-syms identity
                                          :return-strs str)
                                        symbols)))]

    `(map (partial zipmap '~tuple-keys) ~query-form)
    query-form))

(defn- normalize-query [query]
  (if (sequential? query)
    query
    (for [k [:find :keys :syms :strs :with :in :where]
          :when (contains? query k)
          x (cons k (get query k))]
      x)))

(defn- conform-or-throw [spec x]
  (let [c (s/conform spec x)]
    (if (s/invalid? c)
      (throw (ex-info (str "Invalid " (name spec))
                      (s/explain-data spec x)))
      c)))

(defn- top-level-inputs->clj [{:keys [inputs]} {{:keys [inputs-sym src-sym]} :symbols :as ctx}]
  (if inputs
    (->> (for [[idx [in-type in]] (map-indexed vector inputs)]
           (case in-type
             :src-var `[~in (nth ~inputs-sym ~idx)]
             :binding (let [[binding-type binding] in]
                        (case binding-type
                          :bind-scalar [(second binding) `(nth ~inputs-sym ~idx)]
                          :bind-tuple [(mapv second binding) `(nth ~inputs-sym ~idx)]
                          []))
             :rules-var []))
         (reduce into []))
    `[~'$ (nth ~inputs-sym 0)]))

(defn- find-default-src-var [{:keys [inputs]}]
  (if inputs
    (first (for [[in-type in] inputs
                 :when (= :src-var in-type)]
             in))
    '$))

(defn- query->clj [query rules]
  (let [query (->> (normalize-query query)
                   (conform-or-throw ::query))
        name->rules (some->> rules
                             (conform-or-throw ::rule)
                             (group-by (comp :rule-name :rule-head)))
        rule-table-sym (gensym 'rule-table)
        inputs-sym (gensym 'inputs)
        group-sym (gensym '__group)
        src-sym (gensym '__src)
        ctx {:symbols {:rule-table-sym rule-table-sym
                       :inputs-sym inputs-sym
                       :group-sym group-sym
                       :src-sym src-sym}
             :name->rules name->rules}
        {:keys [find-spec return-map with-clause inputs where-clauses]} query
        top-level-inputs (top-level-inputs->clj inputs ctx)
        ctx (assoc ctx :vars (all-vars top-level-inputs))
        clauses (cons [:inputs inputs] (:clauses where-clauses))]
    `(fn [& ~inputs-sym]
       (let [~rule-table-sym {:rec-set #{} :memo-table (atom {})}
             ~@top-level-inputs
             ~src-sym ~(find-default-src-var inputs)]
         (letfn ~(rules->clj ctx)
           ~(->> (wrap-with-find-spec find-spec ctx clauses)
                 (wrap-with-return-map return-map ctx)))))))

(def ^:private memo-compile-query
  (memoize
   (fn [query rules]
     (let [src (query->clj query rules)]
       (eval-internal src)))))

;; Transactions

(s/def :db/id some?)
(s/def :db/entity (s/and (s/map-of keyword? any?)
                         (s/keys :req [:db/id])))
(s/def :db/tx-op (s/or :add (s/cat :op #{:db/add} :e :db/id :a keyword? :v any?)
                       :add-entity :db/entity
                       :retract (s/cat :op #{:db/retract} :e :db/id :a keyword? :v any?)
                       :retract-entity (s/cat :op #{:db.fn/retractEntity} :e :db/id)
                       :retract-attribute (s/cat :op #{:db.fn/retractAttribute} :e :db/id :a keyword?)
                       :cas (s/cat :op #{:db.fn/cas} :e :db/id :a keyword? :old-v any? :new-v any?)
                       :call (s/cat :op #{:db.fn/call} :fn fn? :args (s/* any?))))

(defn- retract-entity-ops [db e]
  (for [[e a v] (datoms db :eav e)]
    [:db/retract e a v]))

(defn- retract-attribute-ops [db e a]
  (for [[e a v] (datoms db :eav e a)]
    [:db/retract e a v]))

(defn- add-entity-ops [db entity]
  (let [e (:db/id entity)]
    (->> (for [[a v] entity
               v (if (set? v)
                   v
                   #{v})]
           (if (s/valid? :db/entity v)
             (cons [:db/add e a (:db/id v)]
                   (add-entity-ops db v))
             [[:db/add e a v]]))
         (reduce into (vec (retract-entity-ops db e))))))

(defn- cas-ops [db e a old-v new-v]
  (let [old-vs (seq (datoms db :eav e a old-v))]
    (if (or old-vs (and (nil? old-v) (empty? old-vs)))
      [[:db/retract e a old-v]
       [:db/add e a new-v]]
      (throw (ex-info (str "CAS failed, not found: " [e a old-v]) {})))))

(declare datom entity)

(defn- apply-tx-data [db tx-data]
  (reduce
   (fn [[db tx-data] tx-op]
     (let [[op-type conformed-tx-op] (conform-or-throw :db/tx-op tx-op)]
       (if (= :call op-type)
         (let [{:keys [fn args]} conformed-tx-op]
           (apply-tx-data db (apply fn db args)))
         (let [{:keys [e a v]} conformed-tx-op
               flat-tx-ops (case op-type
                             :add [tx-op]
                             :add-entity (add-entity-ops db tx-op)
                             :retract [tx-op]
                             :retract-entity (retract-entity-ops db e)
                             :retract-attribute (retract-attribute-ops db e a)
                             :cas (let [{:keys [e a old-v new-v]} conformed-tx-op]
                                    (cas-ops db e a old-v new-v)))
               new-tx-data (vec (for [[op e a v] flat-tx-ops]
                                  (datom e a v (= op :db/add))))]
           [(-with db new-tx-data)
            (reduce conj tx-data new-tx-data)]))))
   [db []]
   tx-data))

(defn- ->tx-report [db-before db-after tx-data]
  {:db-before db-before
   :db-after db-after
   :tx-data tx-data})

;; API

(defn qseq
  "Takes a map of `query` and `args` and returns the lazy sequence of results. See `q`."
  [{:keys [query args]}]
  (let [{:keys [inputs]} (->> (normalize-query query)
                              (conform-or-throw ::query))
        inputs (mapv second (:inputs inputs))
        {rules '%} (zipmap inputs args)]
    (apply (memo-compile-query query rules) args)))

(defn q
  "Executes a `query` with the given `inputs`. See [EDN Datalog](https://docs.datomic.com/on-prem/query/query.html) for reference.
  The return type depends on the find specification.

  Note, pull and lookup-refs aren't supported."
  [query & inputs]
  (let [{:keys [find-spec]} (->> (normalize-query query)
                                 (conform-or-throw ::query))
        [find-type find-spec] (:find-spec find-spec)
        result (qseq {:query query :args inputs})]
    (case find-type
      :find-rel (set result)
      :find-coll (mapv first result)
      :find-scalar (ffirst result)
      :find-tuple (first result))))

(defn datom
  "Low-level function to create a `datom`. By default, `added` is `true`."
  ([e a v]
   (datom e a v true))
  ([e a v added]
   #?(:bb [e a v added]
      :clj (->Datom e a v added)
      :cljs (->Datom e a v added))))

(defn datoms
  "Returns `Datom`s with the fields `[e a v added]`. In Babashka the datoms are vectors.

  Note, retracted datoms are not tracked for the default implementation of db."
  [db index & components]
  (-datoms db index components))

(defn entity
  "Returns a map of all attributes for `eid`.

  Note, unlike Datomic and DataScript, this is a normal map and references cannot be navigated. Lookup-refs aren't supported."
  [db eid]
  (reduce
   (fn [acc [e a v]]
     (update acc a (fn [x]
                     (cond
                       (set? x)
                       (conj x v)

                       (some? x)
                       (conj #{} x v)

                       :else
                       v))))
   nil
   (datoms db :eav eid)))

(defn db
  "Returns the database for the given connection."
  [conn]
  (-db conn))

(defn with
  "Applies the `tx-data` to the immutable `db` and returns a `tx-report`. See `transact!`"
  [db tx-data]
  (let [[db-after tx-data] (apply-tx-data db tx-data)]
    (->tx-report db db-after tx-data)))

(defn db-with
  "Same as `with`, but returns the new `db`."
  [db tx-data]
  (:db-after (with db tx-data)))

(defn transact!
  "Applies the `tx-data` to the current `conn`, updating its database to the new value.

  The `tx-data` is a list of the following:
  - Entity maps containing `:db/id`. Sets and vectors (turned into sets) will be indexed per value. Will retract and replace any current entity. Nested maps with `:db/id` in them will be added as separate entities and referenced.
  - `[:db/add e a v]`
  - `[:db/retract e a v]`
  - `[:db.fn/retractEntity e]`
  - `[:db.fn/retractAttribute e a]`
  - `[:db.fn/cas e a old-v new-v]` - an `old-v` of `nil` also matches no existing attribute.
  - `[:db.fn/call tx-fn & args]` - `tx-fn` is a normal function taking an extra `db` argument before the `args`, returning new a `tx-data` list.

  Returns a `tx-report` containing the keys `[db-before db-after tx-data]` where `tx-data` is the resulting set of `datoms`.

  Note, unlike Datomic and DataScript there's no concept of tx ids, tempids or entids."
  [conn tx-data]
  (let [tx-report (with (db conn) tx-data)]
    (-commit conn tx-report)
    tx-report))

(def ^:private index->datom-fn
  {:eav (fn [e a v]
          (datom e a v))
   :aev (fn [a e v]
          (datom e a v))
   :ave (fn [a v e]
          (datom e a v))
   :vae (fn [v a e]
          (datom e a v))})

;; Default Db implementation.

(defrecord ^:no-doc DbValue [eav aev ave vae]
  Db
  (-with [this tx-data]
    (reduce
     (fn [db [e a v added :as datom]]
       (if added
         (-> db
             (assoc-in [:eav e a v] datom)
             (assoc-in [:aev a e v] datom)
             (assoc-in [:ave a v e] datom)
             (assoc-in [:vae v a e] datom))
         (-> db
             (update-in [:eav e a] dissoc v)
             (update-in [:aev a e] dissoc v)
             (update-in [:ave a v] dissoc e)
             (update-in [:vae v a] dissoc e))))
     this
     tx-data))

  (-datoms [this index components]
    (if-let [idx (get this index)]
      (let [datom-fn (index->datom-fn index)
            [x y z] components]
        (case (count components)
          0 (for [[_ ys] idx
                  [_ zs] ys
                  [_ datom] zs]
              datom)
          1 (for [[_ zs] (get-in idx components)
                  [_ datom] zs]
              datom)
          2 (for [[_ datom] (get-in idx components)]
              datom)
          3 (let [datom (get-in idx components ::not-found)]
              (when-not (= ::not-found datom)
                [datom]))))
      (throw (ex-info (str "Unknown index: " index) {})))))

(defn empty-db
  "Returns an empty database, which by default is a record with the keys `[eav aev ave vae]` storing its indexes."
  []
  (map->DbValue {:eav {} :aev {} :ave {} :vae {}}))

;; Atom connection.

(extend-type #?(:bb Atom :clj IAtom :cljs Atom)
  DbConnection
  (-db [this] @this)

  (-commit [this tx-report]
    (reset! this (:db-after tx-report))))

(defn create-conn
  "Creates a new connection, which is an atom containing the result of a call to `empty-db`."
  []
  (atom (empty-db)))

;; Self-hosted ClojureScript eval.

(defmacro ^:private analyzer-state [[_ ns-sym]]
  `'~(when-let [state (resolve 'cljs.env/*compiler*)]
       (get-in @@state [:cljs.analyzer/namespaces ns-sym])))

#?(:clj (defn- eval-internal [form]
          (eval form))

   :cljs (let [state (or cljs.env/*compiler*
                         (cljs.js/empty-state
                          (fn [state]
                            (assoc-in state [:cljs.analyzer/namespaces 'juxt.trip.core]
                                      (analyzer-state 'juxt.trip.core)))))]
           (defn- eval-internal [form]
             (binding [*ns* (or (find-ns cljs.analyzer/*cljs-ns*)
                                *ns*
                                (find-ns 'juxt.trip.core))
                       cljs.env/*compiler* state
                       cljs.js/*eval-fn* cljs.js/js-eval]
               (eval form)))))

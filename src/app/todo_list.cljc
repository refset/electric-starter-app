(ns app.todo-list

  ; trick shadow into ensuring that client/server always have the same version
  ; all .cljc files containing Electric code must have this line!
  #?(:cljs (:require-macros app.todo-list)) ; <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

  (:require #?(:clj [datascript.core :as d]) ; database on server
            [juxt.trip.core :as t]
            [hyperfiddle.electric :as e]
            [hyperfiddle.electric-dom2 :as dom]
            [hyperfiddle.electric-ui4 :as ui]))

(defonce !conn #?(:clj (t/create-conn) :cljs nil)) ; database on server
(e/def db) ; injected database ref; Electric defs are always dynamic

(comment
  (reset! !conn (t/empty-db))
  )


#?(:clj (defn new-uuid [] (java.util.UUID/randomUUID))
   :cljs (defn new-uuid [] (random-uuid)))

(e/defn TodoItem [id]
  (e/server
   (let [e (t/entity db id)
         status (:task/status e)]
     (e/client
      (dom/div
       (ui/checkbox
        (case status :active false, :done true)
        (e/fn [v]
          (e/server
           (e/discard
            (t/transact! !conn [(assoc (t/entity db id)
                                       :task/status
                                       (if v :done :active))]))))
        (dom/props {:id id}))
       (dom/label (dom/props {:for id}) (dom/text (e/server (:task/description e)))))))))

(e/defn InputSubmit [F]
  ; Custom input control using lower dom interface for Enter handling
  (dom/input (dom/props {:placeholder "Buy milsdfk"})
    (dom/on "keydown" (e/fn [e]
                        (when (= "Enter" (.-key e))
                          (when-some [v (contrib.str/empty->nil (-> e .-target .-value))]
                            (new F v)
                            (set! (.-value dom/node) "")))))))

(e/defn TodoCreate []
  (e/client
    (InputSubmit. (e/fn [v]
                    (e/server
                      (e/discard
                       (t/transact! !conn [{:db/id (new-uuid)
                                             :task/description v
                                             :task/status :active}])))))))

#?(:clj (defn todo-count [db]
          (count
            (t/q '[:find [?e ...] :in $ ?status
                   :where [?e :task/status ?status]] db :active))))

#?(:clj (defn todo-records [db]
          (->> (t/q '[:find [?e #_(pull ?e [:db/id :task/description]) ...]
                      :where [?e :task/status]] db)
               (map (partial t/entity db))
               (sort-by :task/description))))

(e/defn Todo-list []
  (e/server
    (binding [db (e/watch !conn)]
      (e/client
        (dom/link (dom/props {:rel :stylesheet :href "/todo-list.css"}))
        (dom/h1 (dom/text (str "minimal todo list")))
        (dom/p (dom/text "it's masultiplayer, try two tabs"))
        (dom/div (dom/props {:class "todo-list"})
          (TodoCreate.)
          (dom/div {:class "todo-items"}
            (e/server
              (e/for-by :db/id [{:keys [db/id]} (todo-records db)]
                (TodoItem. id))))
          (dom/p (dom/props {:class "counter"})
            (dom/span (dom/props {:class "count"}) (dom/text (e/server (todo-count db))))
            (dom/text " items left")))))))

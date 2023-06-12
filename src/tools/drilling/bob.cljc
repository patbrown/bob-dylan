(ns tools.drilling.bob
  "A simple bulletin board for managing channels in an event bus system. This is extracted from closed source code I've been using for a while. It's not feature complete, because I did not need all the features. But as an open-source library I will make time to add all the HOFs in core.async.

  I use this setup within integrant keys allowing for a visual way to see how channels are connected. Because of the way core.async manages it's mults, taps, pubs and subs, closing almost all the channels occurs by closing the event bus. I've never bothered to add a close function to this library because the wiring is meant to be immutable. All channels are added and wired at integrant init and not intended to be changed after that. Although if your needs differ. As a plus, mixing this with integrant.repl.state and portal makes it dead simple to reason about what is happening in your system."

  (:require [clojure.core.async :as a]
            [clojure.string :refer [blank?]]))

(defn flattenv
  "Flattens a collection and returns a vector"
  [thing]
  (-> thing flatten vec))

(defn extract-chan-ns
  "Takes the channel ns from a channel's coordinates. In the event of nested coordinates, takes the last ns."
  [id] (-> id last namespace keyword))

(def namespace-to-subdir-registry
  {:<bus :buses
   :<mixer :mixers
   :<mixer-output :mixer-outputs
   :<merge :merges
   :<mult :mults
   :<pub :pubs
   :<sub :subs
   :<tap :taps
   :<consumer :consumers})

(defn subdir-for 
  [id]
  (cond
    (not (blank? (-> id last namespace))) (-> id extract-chan-ns namespace-to-subdir-registry)
    (= :dir (first id)) id))

(defn dir-path-for
  ([id]
   (let [id-subdir (subdir-for id)
         sd (if (= :dir (first id))
              id
              [:dir id-subdir id])]
     (flattenv [sd])))
  ([ln id]
   (let [ln-subdir (subdir-for ln)
         id-subdir (subdir-for id)]
     (conj (flattenv [:dir ln-subdir ln id-subdir]) id))))

(defn dir-add
  ([m] (let [{:keys [bob id v ln]} m]
         (if-some [_ ln]
           (dir-add bob ln id v)
           (dir-add bob id v))))
  ([bob id v]
   (assoc-in bob (dir-path-for id) v))
  ([bob ln id v]
   (assoc-in bob (dir-path-for ln id) v)))

(defn add-chan!
  "Generic add channel function. If a channel is provided, it will be used. Otherwise, a new channel will be created."
  ([m] (let [{:keys [bob id ch]} m]
         (if-some [_ ch]
           (add-chan! bob id ch)
           (add-chan! bob id))))
  ([bob ch-id] (add-chan! bob ch-id (a/chan)))
  ([bob ch-id ch]
   (as-> bob $
     (assoc-in $ ch-id ch)
     (assoc-in $ [:dir :chans ch-id] ch)
     (if (= :<bus (extract-chan-ns ch-id))
       (dir-add $ ch-id {:chan ch})
       $))))

(defn add-mult!
  "Creates a new mult, links it to it's source and adds it to the bulletin board."
  ([m] (let [{:keys [bob id ln]} m] (add-mult! bob id ln)))
  ([bob mult-id src-id]
   (let [src-ch (get-in bob src-id)
         mult-ch (a/mult src-ch)]
     (as-> bob $
       (add-chan! $ mult-id mult-ch)
       (dir-add $ mult-id {:chan mult-ch
                           :src src-id
                           :src-ch src-ch})
       (dir-add $ src-id mult-id mult-ch)))))

(defn add-tap!
  "Creates a tap, taps it into a mult and adds it to the bulletin board."
  ([m] (let [{:keys [bob id ln val]} m]
         (if-some [_ val]
           (add-tap! bob id val ln)
           (add-tap! bob id ln))))
  ([bob tap-ident mult-ident]
   (add-tap! bob  tap-ident (a/chan) mult-ident))
  ([bob tap-ident tap-ch mult-ident]
   (let [mult-ch (get-in bob mult-ident)
         _ (a/tap mult-ch tap-ch)]
     (as-> bob $
       (add-chan! $ tap-ident tap-ch)
       (dir-add $ tap-ident {:chan tap-ch
                             :mult mult-ident
                             :mult-chan mult-ch})
       (dir-add $ mult-ident tap-ident tap-ch)))))

(defn add-pub!
  "Adds a pub, linking it to a source, and adds it to the bulletin board."
  ([m] (let [{:keys [bob id ln rule]} m]
         (add-pub! bob id ln rule)))
  ([bob pub-ident src-ident rule]
   (let [src-ch (get-in bob src-ident)
         pub-ch (a/pub src-ch rule)]
     (as-> bob $
       (add-chan! $ pub-ident pub-ch)
       (dir-add $ pub-ident {:chan pub-ch
                             :source src-ident
                             :source-chan src-ch
                             :rule rule})
       (dir-add $ src-ident pub-ident pub-ch)))))

(defn add-sub!
  "Adds a sub to a pub. Must provide an existing channel."
  ([m] (let [{:keys [bob id ln topic]} m] (add-sub! bob id ln topic)))
  ([bob sub-ident pub-ident topic]
   (let [pub-ch (get-in bob pub-ident)
         sub-ch (a/chan)
         _ (a/sub pub-ch topic sub-ch)]
     (as-> bob $
       (add-chan! $ sub-ident sub-ch)
       (dir-add $ sub-ident {:chan sub-ch
                             :pub pub-ident
                             :pub-chan pub-ch
                             :topic topic})
       (dir-add $ pub-ident sub-ident sub-ch)))))

(defn add-merge!
  "Adds a merge of the supplied channels."
  ([m] (let [{:keys [bob id ln]} m] (add-merge! bob id ln)))
  ([bob merge-ident ch-idents]
   (let [chans (mapv #(get-in bob %) ch-idents)
         merge-ch (a/merge chans)
         chs-mapv (apply merge (map (fn [ch-ident chan]
                                        {ch-ident chan}) ch-idents chans))]
     (as-> bob $
       (add-chan! $ merge-ident merge-ch)
       (dir-add $ merge-ident {:chan merge-ch
                               :merges chs-mapv})))))

(defn default-consumer
  ([m] (let [{:keys [ch f]} m] (default-consumer ch f)))
  ([ch f]
   (a/go-loop []
     (when-let [evt (a/<! ch)]
       (f evt)
       (recur)))))

(defn stoppable-consumer [ch f]
  (a/go-loop
   (loop []
     (let [evt (a/<! ch)]
       (when (not= :stop evt)
         (f evt)
         (recur))))))

(def consumer-reg
  {:default default-consumer
   :stoppable stoppable-consumer})

(defn add-consumer!
  "Adds a consumer to a channel."
  ([m] (let [{:keys [bob id ln consumer-type message-fn]} m]
         (add-consumer! bob id ln consumer-type message-fn)))
  ([bob consumer-ident ch-ident consumer-type message-fn]
   (let [ch (get-in bob ch-ident)
         consumer-fn (consumer-type consumer-reg)
         _ (consumer-fn ch message-fn)]
     (as-> bob $
       (add-chan! $ consumer-ident ch)
       (dir-add $ consumer-ident {:chan ch
                                  :consumer-type consumer-type
                                  :message-fn message-fn})
       (dir-add $ ch-ident consumer-ident consumer-fn)))))

(defn- clone-coord-ident-for
  "Used in making mixers."
  [ns full-ident]
  (let [container (butlast full-ident)
        ident (last full-ident)]
    (if (nil? container)
      [(keyword (str  (name ns) "/" (name ident)))]
      (flattenv [container (keyword (str  (name ns) "/" (name ident)))]))))

(defn add-mixer!
  "Adds a mixer and output channel, attaching channels if provided."
  ([m] (let [{:keys [bob id]} m]
         (add-mixer! bob id)))
  ;; TODO add channel on creation
  ([bob mixer-ident]
   (let [mixer-output-ch (a/chan)
         mixer-ch (a/mix mixer-output-ch)
         mixer-output-ident (clone-coord-ident-for :<mixer-output mixer-ident)]
     (as-> bob $
     (add-chan! $ mixer-ident mixer-ch)
     (add-chan! $ mixer-output-ident mixer-output-ch)
     (a/solo-mode mixer-ch :mute)
       (dir-add $ mixer-ident {:chan mixer-ch
                               :mixer-output mixer-output-ident
                               :mixer-output-ch mixer-output-ch
                               :solo-mode :mute})
       (dir-add $ mixer-output-ident {:chan mixer-output-ch
                                      :mixer mixer-ident
                                      :mixer-ch mixer-ch})     
       ))))

(defn add-to-mix!
  "Adds a channel to a mix, setting it's config if provided."
  ([m] (let [{:keys [bob id ln cfg]} m
             conf (or cfg [])]
         (add-to-mix! bob id ln conf)))
  ([bob chan-ident mixer-ident]
   (add-to-mix! bob chan-ident mixer-ident {}))
  ([bob chan-ident mixer-ident toggle-config]
   (let [ch-to-add (get-in bob chan-ident)
         mixer-ch (get-in bob mixer-ident)]
     (as-> bob $
       (a/admix mixer-ch ch-to-add)
     (when-not (empty? toggle-config) (a/toggle mixer-ch {ch-to-add toggle-config}))
     (dir-add $ chan-ident mixer-ident {:chan mixer-ch
                                          :config toggle-config})
     (dir-add $ mixer-ident chan-ident {:chan mixer-ch
                                          :config toggle-config}))
     )))

(defn toggle-chan!
  "Changes the mix settings on a channel."
  ([m] (let [{:keys [bob id ln cfg]} m]
         (toggle-chan! bob id ln cfg)))
  ([bob mixer-ident chan-ident toggle-config]
   (let [ch-to-toggle (get-in @bob chan-ident)
         mixer-ch (get-in @bob mixer-ident)]
     (as-> bob $
       (a/toggle mixer-ch {ch-to-toggle toggle-config})
     (dir-add $ chan-ident mixer-ident {:chan mixer-ch
                                          :config toggle-config})
     (dir-add $ mixer-ident chan-ident {:chan mixer-ch
                                          :config toggle-config})))))

(defn pause-chan!
  "Pauses a channel in a mix."
  ([m] (let [{:keys [bob id ln]} m] (pause-chan! bob id ln)))
  ([bob mixer-ident chan-ident]
   (toggle-chan! bob mixer-ident chan-ident {:mute false
                                            :pause true
                                            :solo false})))

(defn mute-chan!
  "Mutes a channel in a mix."
  ([m] (let [{:keys [bob id ln]} m] (mute-chan! bob id ln)))
  ([bob mixer-ident chan-ident]
   (toggle-chan! bob mixer-ident chan-ident {:mute true
                                            :pause false :solo false})))

(defn solo-chan!
  "Solos a channel in a mix."
  ([m] (let [{:keys [bob id ln]} m] (solo-chan! bob id ln)))
  ([bob mixer-ident chan-ident]
   (toggle-chan! bob mixer-ident chan-ident {:mute false
                                            :pause false
                                            :solo true})))

(def ns->creation-f-registry
  {:<chan add-chan!
   :<bus add-chan!
   :<mult add-mult!
   :<tap add-tap!
   :<pub add-pub!
   :<sub add-sub!
   :<merge add-merge!
   :<mixer add-mixer!
   :<consumer add-consumer!})

(defn id->f
  "Takes an id and returns the function that creates, wires, and adds it to the bulletin board."
  [id]
  (let [idns  (extract-chan-ns id)]
    (idns ns->creation-f-registry)))


(defn bob!
  "Wires a channel into the bulletin board."
  [& ms]
  (let [m (apply merge ms)
        {:keys [id]} m
        f (id->f id)]
    (f m)))

(defn add-to-bob!
  "Wires everything into bulletin board. Comms arg is a vector. The order of this vector should be starting with your event-bus. :buses, taps, mults, pubs, subs, merges, consumers. Often nothing bad will happen if you don't do this, but it's good practice because when you don't you may wind up creating a new channel when you'd prefer to use an old one and there's no easy way to troubleshoot that one."
  [bob comms]
  (loop [chs (seq (flattenv comms))
         results [bob]]
    (if chs
      (let [ch (first chs)
            working-bob (last results)
            new-result (bob! {:bob working-bob} ch)]
        (recur (next chs) (conj results new-result)))
      (last results))))

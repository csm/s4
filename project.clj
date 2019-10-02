(defproject s4 "0.1.0"
  :description "Simulated S3"
  :url "https://github.com/csm/s4"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [aleph "0.4.6"]
                 [io.replikativ/konserve "0.6.0-SNAPSHOT"]
                 [io.replikativ/superv.async "0.2.9"]
                 [com.arohner/uri "0.1.2"]
                 [org.clojure/data.xml "0.2.0-alpha6"]]
  :profiles {:test {:dependencies [[ch.qos.logback/logback-classic "1.1.8"]
                                   [ch.qos.logback/logback-core "1.1.8"]]
                    :resource-paths ["test-resources"]}}
  :repl-options {:init-ns s4.repl})

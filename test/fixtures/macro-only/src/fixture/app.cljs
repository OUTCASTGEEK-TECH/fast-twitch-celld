(ns fixture.app
  "Authoring fixture intentionally requires only macros."
  (:require-macros [fast-twitch.celld.macros :refer [defcell defrpc]]))

(defrpc ping {:method :ping :args [:cat] :returns :string} [_ctx] "pong")

(defcell Minimal {:binding :MINIMAL :include [ping]})

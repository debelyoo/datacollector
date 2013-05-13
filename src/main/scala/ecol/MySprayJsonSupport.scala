package ecol

trait MySprayJsonSupport extends spray.httpx.SprayJsonSupport {
  import spray.json._

  implicit object JsObjectWriter extends RootJsonFormat[JsObject] {
    def write(jsObject: JsObject) = jsObject
    def read(value: JsValue) = value.asJsObject
  }

  implicit object JsArrayWriter extends RootJsonFormat[JsArray] {
    def write(jsArray: JsArray) = jsArray
    def read(value: JsValue) = value.asInstanceOf[JsArray]
  }
}

object MySprayJsonSupport extends MySprayJsonSupport
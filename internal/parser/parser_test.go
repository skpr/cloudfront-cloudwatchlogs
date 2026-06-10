package parser

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseLogGroupAndStream(t *testing.T) {
	logGroup := GetLogGroupName("/skpr/my-cluster/my-project/dev/E38J4Y0L8GXH9D.2020-06-08-07.d51ccc94.gz")
	assert.Equal(t, "/skpr/my-cluster/my-project/dev", logGroup)

	// Make sure logGroup gets prefixed with a /.
	logGroup = GetLogGroupName("skpr/my-cluster/my-project/prod/E38J4Y0L8GXH9D.2020-06-08-07.d51ccc94.gz")
	assert.Equal(t, "/skpr/my-cluster/my-project/prod", logGroup)
}

func TestParseDateAndMessageLegacy(t *testing.T) {
	line := "2020-06-18	03:38:13	SYD4-C2	35207	111.111.11.1	GET	asdasdasd.cloudfront.net	/admin/people	200	https://example.com/home	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_14_5)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/83.0.4103.97%20Safari/537.36	-	-	Miss	oe49fbR4FcmNWieL3CVBnkQFZiNls0O9Zg24IfUYPWOXMX36hqQI4g==	dev.snsw-cos.snsw.skpr.dev	https	45	0.301	-	TLSv1.2	ECDHE-RSA-AES128-GCM-SHA256	Miss	HTTP/2.0	-	-	57856	0.299	Miss	text/html;%20charset=UTF-8	-	-	-"
	expectedDate, _ := time.Parse("2006-01-02 15:04:05", "2020-06-18 03:38:13")
	date, message, err := ParseDateAndMessageLegacy(line)
	assert.Nil(t, err)
	assert.Equal(t, expectedDate, date)
	expectedMessage := "SYD4-C2	35207	111.111.11.1	GET	asdasdasd.cloudfront.net	/admin/people	200	https://example.com/home	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_14_5)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/83.0.4103.97%20Safari/537.36	-	-	Miss	oe49fbR4FcmNWieL3CVBnkQFZiNls0O9Zg24IfUYPWOXMX36hqQI4g==	dev.snsw-cos.snsw.skpr.dev	https	45	0.301	-	TLSv1.2	ECDHE-RSA-AES128-GCM-SHA256	Miss	HTTP/2.0	-	-	57856	0.299	Miss	text/html;%20charset=UTF-8	-	-	-"
	assert.Equal(t, expectedMessage, message)
}

func TestParseDateAndMessageJson(t *testing.T) {
	line := "{\"date\":\"2026-06-10\",\"time\":\"00:03:12\",\"x-edge-location\":\"SYD62-P1\",\"sc-bytes\":14823,\"c-ip\":\"203.22.104.51\",\"cs-method\":\"GET\",\"cs(Host)\":\"d1a2b3c4d5e6f7.cloudfront.net\",\"cs-uri-stem\":\"/\",\"sc-status\":200,\"cs(Referer)\":\"-\",\"cs(User-Agent)\":\"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36\",\"cs-uri-query\":\"-\",\"cs(Cookie)\":\"SESS8f3a2b1c=abc123xyz; has_js=1\",\"x-edge-result-type\":\"Miss\",\"x-edge-request-id\":\"gHk7Lm2NpQrStUvWxYz1A2B3C4D5E6F7\",\"x-host-header\":\"www.example.com.au\",\"cs-protocol\":\"https\",\"cs-bytes\":521,\"time-taken\":0.243,\"x-forwarded-for\":\"203.22.104.51\",\"ssl-protocol\":\"TLSv1.3\",\"ssl-cipher\":\"TLS_AES_128_GCM_SHA256\",\"x-edge-response-result-type\":\"Miss\",\"cs-protocol-version\":\"HTTP/2.0\",\"fle-status\":\"-\",\"fle-encrypted-fields\":\"-\",\"c-port\":54321,\"time-to-first-byte\":0.201,\"x-edge-detailed-result-type\":\"Miss\",\"sc-content-type\":\"text/html; charset=UTF-8\",\"sc-content-len\":14501,\"sc-range-start\":\"-\",\"sc-range-end\":\"-\",\"CloudFront-Viewer-Address\":\"203.22.104.51:54321\",\"CloudFront-Viewer-JA4-Fingerprint\":\"t13d1516h2_8daaf6152771_b0da82dd1658\"}"
	expectedDate, _ := time.Parse("2006-01-02 15:04:05", "2026-06-10 00:03:12")
	date, message, err := ParseDateAndMessageJson(line)
	assert.Nil(t, err)
	assert.Equal(t, expectedDate, date)
	expectedMessage := "{\"date\":\"2026-06-10\",\"time\":\"00:03:12\",\"x-edge-location\":\"SYD62-P1\",\"sc-bytes\":14823,\"c-ip\":\"203.22.104.51\",\"cs-method\":\"GET\",\"cs(Host)\":\"d1a2b3c4d5e6f7.cloudfront.net\",\"cs-uri-stem\":\"/\",\"sc-status\":200,\"cs(Referer)\":\"-\",\"cs(User-Agent)\":\"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36\",\"cs-uri-query\":\"-\",\"cs(Cookie)\":\"SESS8f3a2b1c=abc123xyz; has_js=1\",\"x-edge-result-type\":\"Miss\",\"x-edge-request-id\":\"gHk7Lm2NpQrStUvWxYz1A2B3C4D5E6F7\",\"x-host-header\":\"www.example.com.au\",\"cs-protocol\":\"https\",\"cs-bytes\":521,\"time-taken\":0.243,\"x-forwarded-for\":\"203.22.104.51\",\"ssl-protocol\":\"TLSv1.3\",\"ssl-cipher\":\"TLS_AES_128_GCM_SHA256\",\"x-edge-response-result-type\":\"Miss\",\"cs-protocol-version\":\"HTTP/2.0\",\"fle-status\":\"-\",\"fle-encrypted-fields\":\"-\",\"c-port\":54321,\"time-to-first-byte\":0.201,\"x-edge-detailed-result-type\":\"Miss\",\"sc-content-type\":\"text/html; charset=UTF-8\",\"sc-content-len\":14501,\"sc-range-start\":\"-\",\"sc-range-end\":\"-\",\"CloudFront-Viewer-Address\":\"203.22.104.51:54321\",\"CloudFront-Viewer-JA4-Fingerprint\":\"t13d1516h2_8daaf6152771_b0da82dd1658\"}"
	assert.Equal(t, expectedMessage, message)
}

func TestParseDateTime(t *testing.T) {
	date, err := parseDateTime("2020-06-18", "03:38:13")
	assert.Nil(t, err)
	assert.Equal(t, date, time.Date(2020, 6, 18, 3, 38, 13, 0, time.UTC))
}

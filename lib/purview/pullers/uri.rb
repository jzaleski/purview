module Purview
  module Pullers
    class URI < Base
      def pull(window)
        request = windowed_request(window)
        with_context_logging("`pull` from: #{request.path}") do
          http.request(request).body
        end
      end

      private

      def basic_auth?
        username && password
      end

      def host
        uri.host
      end

      def http
        Net::HTTP.new(host, port).tap do |http|
          if https?
            http.use_ssl = true
            http.verify_mode = OpenSSL::SSL::VERIFY_NONE
          end
        end
      end

      def https?
        uri.scheme == 'https'
      end

      def password
        opts[:password]
      end

      def port
        uri.port
      end

      def uri
        ::URI.parse(opts[:uri])
      end

      def username
        opts[:username]
      end

      def windowed_request(window)
        Net::HTTP::Get.new(windowed_request_uri(window)).tap do |request|
          if basic_auth?
            request.basic_auth(username, password)
          end
        end
      end

      def windowed_request_uri(window)
        uri.to_s.tap do |request_uri|
          request_uri << (request_uri.include?('?') ? '&' : '?')
          request_uri << 'ts1=%s&ts2=%s' % [window.min.to_i, window.max.to_i]
        end
      end
    end
  end
end

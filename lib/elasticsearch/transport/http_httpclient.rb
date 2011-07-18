# require 'patron'
require 'httpclient'
require 'cgi'

module ElasticSearch
  module Transport
    class HTTPHTTPClient < Base

      DEFAULTS = {
        :timeout => 5
      }.freeze

      def initialize(server, options={})
        super
        @options = DEFAULTS.merge(@options)
      end

      def connect!
        @session = HTTPClient.new
      end

      def all_nodes
        http_addresses = nodes_info([])["nodes"].collect { |id, node| node["http_address"] }
        http_addresses.collect! do |a|
          if a =~ /inet\[.*\/([\d.:]+)\]/
            $1
          end
        end.compact!
        http_addresses
      end

      private

      def request(method, operation, params={}, body=nil, headers={})
        begin
          uri = generate_uri(operation)
          query = generate_query_string(params)
          path = [uri, query].join("?")
          response = @session.send(method.to_sym, "http://#{@server}#{path}", body, headers)
          handle_error(response) if response.status >= 500
          response
        rescue Exception => e
          raise e
        end
      end
    end
  end
end

# include thrift-generated code
$:.push('../thrift-generated-src-ruby', __FILE__)

require 'thrift'
require 'information_center'
require 'pp'

begin
  port = ARGV[0]

  transport = Thrift::FramedTransport.new(Thrift::Socket.new('localhost', port))
  protocol = Thrift::BinaryProtocol.new(transport)
  client = InformationCenter::Client.new(protocol)

  transport.open()

  req = DeleteAccountRequest.new()
  req.deletingAccountId = ARGV[1].to_i
  req.accountId = ARGV[2].to_i

  pp req
  result = client.deleteAccount(req)
  pp result
  
  transport.close()
rescue
  puts $!
end

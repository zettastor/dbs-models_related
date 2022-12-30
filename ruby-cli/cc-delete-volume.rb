# include thrift-generated code
$:.push('../thrift-generated-src-ruby', __FILE__)

require 'thrift'
require 'control_center'
require 'pp'

begin
  port = ARGV[0]

  transport = Thrift::FramedTransport.new(Thrift::Socket.new('localhost', port))
  protocol = Thrift::BinaryProtocol.new(transport)
  client = ControlCenter::Client.new(protocol)

  transport.open()

  req = DeleteVolumeRequest.new()    
  req.volumeId = ARGV[1].to_i
  req.accountId = ARGV[2].to_i

  pp req
  result = client.deleteVolume(req)
  pp result
  
  transport.close()
rescue
  puts $!
end

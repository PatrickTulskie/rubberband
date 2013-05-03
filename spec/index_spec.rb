require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe "index ops" do
  before(:all) do
    @first_index = 'first-' + Time.now.to_i.to_s
    @second_index = 'second-' + Time.now.to_i.to_s
    @client = ElasticSearch.new('http://127.0.0.1:9200', :index => @first_index, :type => "tweet")
  end

  after(:all) do
    @client.delete_index(@first_index)
    @client.delete_index(@second_index)
    sleep(1)
  end

  it "should get and delete a document" do
    @client.index({:foo => "bar"}, :id => "1", :refresh => true)

    @client.get("1").foo.should == "bar"
    @client.delete("1", :refresh => true).should be_true
    @client.get("1").should be_nil
  end
  
  it "should not delete when passed a nil id" do
    @client.index({:foo => "bar"}, :id => "1", :refresh => true)

    @client.delete(nil).should be_false
    @client.get("1").should_not be_nil
  end

  it 'should search and count documents' do
    @client.index({:foo => "bar"}, :id => "1")
    @client.index({:foo => "baz"}, :id => "2")
    @client.index({:foo => "baz also"}, :id => "3")
    @client.refresh(@first_index)

    @client.search("bar").should have(1).items
    @client.count("bar").should == 1

    @client.search(:query => { :term => { :foo => 'baz' }}).should have(2).items
    @client.count(:term => { :foo => 'baz' }).should == 2

    @client.search(:size => 1, :query => { :term => { :foo => 'baz' }}).should have(1).items
  end

  it 'should return ids when given :ids_only' do
    @client.index({:socks => "stripey"}, :id => "5")
    @client.index({:stripey => "stripey too"}, :id => "6")
    @client.refresh

    results = @client.search({:query => { :field => { :socks => 'stripey' }}}, :ids_only => true)
    results.should include("5")
  end

  it 'should delete by query' do
    @client.index({:deleted => "bar"}, :id => "d1")
    @client.index({:deleted => "bar"}, :id => "d2")

    @client.index({:deleted => "bar"}, :id => "d3", :index => @second_index)
    @client.refresh(:all)

    @client.count(:term => { :deleted => 'bar'}).should == 2
    @client.count({:term => { :deleted => 'bar'}}, :index => @second_index).should == 1
    @client.delete_by_query(:term => { :deleted => 'bar' })
    @client.refresh(:all)
    @client.count(:term => { :deleted => 'bar'}).should == 0
    @client.count({:term => { :deleted => 'bar'}}, :index => @second_index).should == 1
  end

  it 'should delete by query across indices and types' do
    @client.index({:deleted => "baz"}, :id => "d1")
    @client.index({:deleted => "baz"}, :id => "d2")

    @client.index({:deleted => "baz"}, :id => "d3", :index => @second_index)
    @client.refresh(:all)

    @client.count(:term => { :deleted => 'baz'}).should == 2
    @client.count({:term => { :deleted => 'baz'}}, :index => @second_index).should == 1

    # create a non-scoped client
    @client2 = ElasticSearch.new('http://127.0.0.1:9200')
    @client2.delete_by_query(:term => { :deleted => 'baz' })

    @client.refresh(:all)
    @client.count(:term => { :deleted => 'baz'}).should == 0
    @client.count({:term => { :deleted => 'baz'}}, :index => @second_index).should == 0
  end

  it 'should perform a successful multi get with an array' do
    @client.index({:foo => "bar"}, :id => "1")
    @client.index({:foo => "baz"}, :id => "2")
    @client.index({:foo => "bazbar"}, :id => "3")
    ids = ["1", "2", "3"]
    results = @client.multi_get(ids).inject([]) { |r,e| r << e.id }
    results.should == ids
  end
 
  it 'should perform a successful multi get' do
    @client.index({:foo => "bar", :bar => "boo1"}, :id => "1")
    @client.index({:foo => "baz", :bar => "boo2"}, :id => "2")
    @client.index({:foo => "bazbar", :bar => "boo3"}, :id => "3")
    query = {
      "docs" => [
        { "_id" => "1", "fields" => [] },
        { "_id" => "2" },
        { "_id" => "3", "fields" => ["foo"] },
      ]
    }
    results = @client.multi_get(query).inject([]) do
      |r,e| r << { "id" => e.id, "fields" => e.fields, "_source" => e._source }
    end
    results.should == [
      { "id" => "1", "fields" => nil, "_source" => nil },
      { "id" => "2", "fields" => nil, "_source" => { "foo" => "baz", "bar" => "boo2" } },
      { "id" => "3", "fields" => { "foo" => "bazbar" }, "_source" => nil },
    ]
  end

  it 'should handle html escaping and unescaping' do
    @client.index({'fo/o' => 'ba=r'}, :id => "1'")
    @client.refresh
    
    results = @client.search({:query => { :field => { 'fo/o' => 'ba=r' }}})
    results.should have(1).item
    results.first.id.should == "1'"
    results.first._source.should == {'fo/o' => 'ba=r'}
  end
end

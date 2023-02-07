# frozen_string_literal: true

namespace(:worker) do
  desc("Run the worker")
  task(run: :environment) do
    # See https://googleapis.dev/ruby/google-cloud-pubsub/latest/index.html

    puts("Worker starting...")

    # Block, letting processing threads continue in the background
    pubsub = Pubsub.new.client
    topics = pubsub.topics

    puts("Topics in project:")
    topics.each do |topic|
      puts(topic.name)
    end

    topic = Pubsub.new.topic("projects/code-challenge/topics/challenge")
    puts(topic)
    subscription = topic.subscribe("whatever")
    if subscription.nil?
      puts("Subscription not found: 'whatever'")
    else
      subscriber = subscription.listen do |message|
        # job = ActiveJob::DeserializationError.wrap(message) do
        #   ActiveJob::Base.deserialize(message.data)
        # end
        # binding.pry
        puts(message.data)
        job = ActiveJob::Base.deserialize(JSON.parse(message.data))
        puts("processing job...")
        job.perform_now
        message.acknowledge!
      end
      puts("starting subscriber")

      subscriber.on_error do |error|
        puts(error)
      end

      subscriber.start
    end

    sleep
  end
end

from common import message_protocol


class MessageHandler:
    next_id = 0

    def __init__(self):
        self.client_id = MessageHandler.next_id
        MessageHandler.next_id += 1
        self.message_count = 0
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.message_count += 1
        return message_protocol.internal.serialize(["DATA", self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize(["EOF", self.client_id, self.message_count])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        client_id, fruit_top = fields[0], fields[1]
        if client_id != self.client_id:
            return None
        return fruit_top

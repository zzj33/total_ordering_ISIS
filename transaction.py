# a comparable and hashable transaction (by (priority, node_id))
# use message to construct a transaction

class Transaction:
    def __init__(self, message):
        self.trans_info = message.transaction
        self.priority = message.priority      # proposed priority
        self.node_id = message.node_id        # proposed node_id
        self.deliverable = False
        self.id = (message.trans_id.ori_priority, message.trans_id.ori_node_id)

    def __lt__(self, other):
        return self.node_id < other.node_id if self.priority == other.priority \
            else self.priority < other.priority

    def __eq__(self, other):
        return (self.priority == other.priority) & (self.node_id == other.node_id)

    def __hash__(self):
        return hash((self.priority, self.node_id))

    def set_priority(self, priority, node_id):
        if (priority > self.priority) | (priority == self.priority & node_id > self.node_id):
            self.priority = priority
            self.node_id = node_id



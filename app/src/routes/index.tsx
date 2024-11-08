import { createFileRoute } from '@tanstack/react-router'
import { trpc } from '../utils/trpc'

export const Route = createFileRoute('/')({
  component: HomeComponent,
})

function HomeComponent() {
  const utils = trpc.useUtils();

  const usersQuery = trpc.userList.useQuery();
  const userCreator = trpc.userCreate.useMutation({
    onSuccess(_) {
      utils.userList.invalidate();
    }
  });

  return (
    <div className="p-2">
      <h3>Welcome Home!</h3>

      <button onClick={() => userCreator.mutate({ name: 'Frodo' })}>
        Create User!
      </button>

      {usersQuery.data?.map(user => (<p key={user.id}>{user.id}: {user.name}</p>))}
    </div>
  )
}
